def load_kernels(command):
    import sqlite3, subprocess, tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        dump_name = tmpdir + "/dump"
        subprocess.run(["nsys", "profile", "--output", dump_name, "--export", "sqlite"] + command)
        connection = sqlite3.connect(dump_name + ".sqlite")
        cursor = connection.cursor()
        string_dict = dict(cursor.execute("SELECT * FROM StringIds").fetchall())
        raw_kernels = cursor.execute("SELECT start, end, shortName FROM CUPTI_ACTIVITY_KIND_KERNEL ORDER BY start").fetchall()
        connection.close()
        return [(k[0], k[1], string_dict[k[2]]) for k in raw_kernels]

def sample_kernels(kernels, sample_count):
    # Kernels should have shape (begin, end, name) and sorted by start, assuming no overlapping
    start, stop = kernels[0][0], kernels[-1][1]
    sample_times = [start+(i+0.5)*(stop-start)/sample_count for i in range(sample_count)]
    skip_history = dict((kn, 0) for kn in set(k[2] for k in kernels))
    sample_kernels = list()
    for k in kernels:
        if len(sample_kernels) == len(sample_times):
            break
        if k[1] > sample_times[len(sample_kernels)]:
            sample_kernels.append((k[2], skip_history[k[2]]))
        skip_history[k[2]] += 1
    return [s / 1e9 for s in sample_times], sample_kernels

def profile_kernel(command, kernel, skip):
    import csv, io, subprocess
    ncu_command = ["ncu", "--csv", "--target-process", "all", "--kernel-name", str(kernel), "--launch-skip", str(skip), "--launch-count", "1"] + command
    raw_result = subprocess.check_output(ncu_command).decode("utf-8")
    result = raw_result[raw_result.index("\"ID\""):]
    string_stream = io.StringIO(result)
    csv_reader = csv.DictReader(string_stream)
    measure = (None, None)
    for row in csv_reader:
        if row["Section Name"] == "GPU Speed Of Light":
            if row["Metric Name"] == "Memory [%]":
                measure = (row["Metric Value"], measure[1])
            elif row["Metric Name"] == "SM [%]":
                measure = (measure[0], row["Metric Value"])
    return measure

if __name__ == "__main__":
    import argparse, csv
    parser = argparse.ArgumentParser()
    parser.add_argument("command", nargs="*")
    parser.add_argument("-o", "--output", default="record.csv")
    parser.add_argument("-s", "--sample_count", default=10, type=int)
    args = parser.parse_args()
    kernels = load_kernels(args.command)
    sample_times, sample_kernels = sample_kernels(kernels, args.sample_count)
    memory, sm = zip(*[profile_kernel(args.command, k[0], k[1]) for k in sample_kernels])
    with open(args.output, "w", newline="") as helicorder:
        csv_writer = csv.writer(helicorder)
        csv_writer.writerow(("Timestamp (s)", "Memory (%)", "SM (%)"))
        for row in zip(sample_times, memory, sm):
            csv_writer.writerow(row)