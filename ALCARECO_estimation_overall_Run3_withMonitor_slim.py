# author: Jordan Martins
import subprocess
import json
from tqdm import tqdm
from collections import defaultdict


def get_das_data(query):
    try:
        result = subprocess.run(
            ['dasgoclient', '--query', query, '--json'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
        data = result.stdout.decode('utf-8')
        return json.loads(data)
    except subprocess.CalledProcessError as e:
        print(f"Error executing dasgoclient: {e.stderr.decode()}")
        return None


def get_datasets(pattern):
    query = f"dataset dataset={pattern} instance=prod/global"
    print(f"Executing query: {query}")  # Debugging information
    data = get_das_data(query)

    if not data:
        print(f"No data found for pattern {pattern}")
        return []

    datasets = []
    try:
        for entry in data:
            for ds_info in entry['dataset']:
                datasets.append(ds_info['name'])
        return datasets
    except (KeyError, IndexError, TypeError) as e:
        print(f"Error processing data: {e}")
        return []


def get_dataset_info(dataset):
    query = f"dataset dataset={dataset} instance=prod/global"
    data = get_das_data(query)

    if not data:
        return None, None

    dataset_size = None
    num_events = None

    try:
        for entry in data:
            for ds_info in entry['dataset']:
                if 'size' in ds_info and 'nevents' in ds_info:
                    dataset_size = ds_info['size']
                    num_events = ds_info['nevents']
                    return dataset_size, num_events
    except (KeyError, IndexError, TypeError) as e:
        print(f"Error processing data: {e}")
        return None, None


def process_datasets(group_name, patterns):
    results = {
        'total_datasets': 0,
        'total_size': 0,
        'total_events': 0,
        'size_by_year': defaultdict(int),
        'count_by_year': defaultdict(int)
    }

    for pattern in patterns:
        datasets = get_datasets(pattern)

        for dataset in tqdm(datasets, desc=f"Processing datasets for {group_name}"):
            size, events = get_dataset_info(dataset)
            if size is not None and events is not None:
                results['total_datasets'] += 1
                results['total_size'] += size
                results['total_events'] += events

                if 'Run2022' in dataset:
                    results['size_by_year']['2022'] += size
                    results['count_by_year']['2022'] += 1
                elif 'Run2023' in dataset:
                    results['size_by_year']['2023'] += size
                    results['count_by_year']['2023'] += 1
                elif 'Run2024' in dataset:
                    results['size_by_year']['2024'] += size
                    results['count_by_year']['2024'] += 1

    return results


def main():
    group_patterns = {
        'PPS': ["/*/Run202*PPSCalMaxTracks*PromptReco*/ALCA*"],
        'TkAl': [
            "/*/Run202*-TkAlCosmics0T-PromptReco*/ALCARECO",
            "/*/Run202*-TkAlCosmicsInCollisions-PromptReco-v*/ALCARECO",
            "/*/Run202*-TkAlMinBias-PromptReco*/ALCARECO",
            "/*/Run202*-TkAlZMuMu-PromptReco*/ALCARECO",
            "/*/Run202*-TkAlMuonIsolated-PromptReco*/ALCARECO",
            "/*/Run202*-TkAlDiMuonAndVertex-PromptReco*/ALCARECO",
            "/*/Run202*-TkAlUpsilonMuMu-PromptReco*/ALCARECO",
            "/*/Run202*-TkAlJpsiMuMu-PromptReco*/ALCARECO",
            "/*/Run202*-TkAlJetHT-PromptReco*/ALCARECO",
            "/*/Run202*-TkAlV0s-PromptReco*/ALCARECO/"
        ],
        'BRIL': [
            "/*/Run202*-AlCaPCCZeroBias-PromptReco*/ALCARECO",
            "/*/Run202*-AlCaPCCRandom-PromptReco*/ALCARECO"
        ],
        'ECAL': [
            "/*/Run202*-EcalUncalWElectron-PromptReco*/ALCARECO",
            "/*/Run202*-EcalUncalZElectron-PromptReco*/ALCARECO"
        ],
        'Pixel': [
            "/*/Run202*SiPixelCalSingleMuon*PromptReco*/ALCARECO",
            "/*/Run202*SiPixelCalSingleMuonTight*PromptReco*/ALCARECO"
        ]
    }

    group_results = {}

    for group_name, patterns in group_patterns.items():
        group_results[group_name] = process_datasets(group_name, patterns)

    for group_name, results in group_results.items():
        total_size_tb = results['total_size'] / \
            (1024**4)  # Convert bytes to terabytes
        average_size_per_dataset_tb = total_size_tb / \
            results['total_datasets'] if results['total_datasets'] > 0 else 0
        average_size_per_event_tb = total_size_tb / \
            results['total_events'] if results['total_events'] > 0 else 0

        print(f"\nGroup: {group_name}")
        print(f"Total number of datasets: {results['total_datasets']}")
        print(f"Total size of all datasets: {total_size_tb:.2f} TB")

        for year in ['2022', '2023', '2024']:
            print(
                f"Total number of datasets in {year}: {results['count_by_year'][year]}")
            print(
                f"Total size of datasets in {year}: {results['size_by_year'][year] / (1024**4):.2f} TB")

        print(
            f"Average size per dataset: {average_size_per_dataset_tb:.2f} TB")
        print(f"Average size per event: {average_size_per_event_tb:.12f} TB")


if __name__ == "__main__":
    main()
