from exptlib.directory import DirectoryTree
from collections import defaultdict


def metadata_from_directory(directory):
    d = defaultdict(lambda: defaultdict(list))
    for f in directory.glob("*"):
        trial_parts = f.stem.split("_")
        fish_name = list(filter(lambda x: x.startswith("fish"), trial_parts)).pop()
        for l in [lambda x: x.startswith("trial"), lambda x: all([i.isdigit() for i in x])]:
            try:
                trial_index = list(filter(l, trial_parts)).pop()
                idx = trial_parts.index(trial_index)
                trial_name = "_".join(trial_parts[:idx + 1])
                break
            except IndexError:
                continue
        else:  # does not break
            d[fish_name][f.stem] = f
            continue
        d[fish_name][trial_name].append(f)
    return d


if __name__ == "__main__":
    directory = r"D:\DATA\imaging\mafaa"
    structure = DirectoryTree(directory, levels=("data_type", "date"), map_data=metadata_from_directory)
    for *_, path in structure:
        for fish, trials in path.data.items():
            print(fish, list(trials.keys()))
