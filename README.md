# HVU-Downloader
HVU Downloader tool
# Usage
First, clone this repository.
```
git clone https://github.com/holistic-video-understanding/HVU-Downloader.git
cd HVU-Downloader
```

Next, setup the conda environment:
```
conda env create -f environment.yml
source activate HVU
pip3 install --upgrade youtube-dl
```

Finally, download a dataset split by calling:
```
mkdir <data_dir>; python HVU_download.py {dataset_split}.csv <data_dir>
```

# Acknowledgment
The HVU downloader tool is a modified version of the ActivityNet team downloader tool for Kinetics dataset:
```
https://github.com/activitynet/ActivityNet/tree/master/Crawler/Kinetics
```
