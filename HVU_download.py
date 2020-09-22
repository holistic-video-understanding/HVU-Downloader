import argparse
import glob
import json
import os
import shutil
import subprocess
import uuid
from collections import OrderedDict

from joblib import delayed
from joblib import Parallel
import pandas as pd
from moviepy.video.io.ffmpeg_tools import ffmpeg_extract_subclip as trim


def construct_video_filename(row,output_dir, trim_format='%06d'):
    """Given a dataset row, this function constructs the
       output filename for a given video.
    """
    basename = '%s_%s_%s.mp4' % (row['video-id'],
                                 trim_format % row['start-time'],
                                 trim_format % row['end-time'])
    output_filename = os.path.join(output_dir,basename)
    return output_filename

def trim_video(row, output_dir, trim_format = '%06d'):
    """Trim all the videos present in the dataset if they were downloaded successfully"""
    output_filename = construct_video_filename(row, output_dir, trim_format)
    trimmed_filename = output_filename.split('.mp4')[0] + '_.mp4'
    start_time = row['start-time']
    end_time = row['end-time']

    if os.path.exists(output_filename):
        trim(output_filename,start_time,end_time,trimmed_filename)
        os.remove(output_filename)
    else:
        print("Video not found!\n")
    return

def download_clip(video_identifier, output_filename,
                  start_time, end_time,
                  num_attempts=5,
                  url_base='https://www.youtube.com/watch?v='):
    """Download a video from youtube if exists and is not blocked.

    arguments:
    ---------
    video_identifier: str
        Unique YouTube video identifier (11 characters)
    output_filename: str
        File path where the video will be stored.
    start_time: float
        Indicates the begining time in seconds from where the video
        will be trimmed.
    end_time: float
        Indicates the ending time in seconds of the trimmed video.
    """
    # Defensive argument checking.
    assert isinstance(video_identifier, str), 'video_identifier must be string'
    assert isinstance(output_filename, str), 'output_filename must be string'
    assert len(video_identifier) == 11, 'video_identifier must have length 11'

    status = False
    # Construct command line for getting the direct video link.
    command = ['youtube-dl',
               '--force-ipv4',
               '--quiet', '--no-warnings',
               '-f', 'mp4',
               '-o', '"%s"' % output_filename,
               '"%s"' % (url_base + video_identifier)]
    command = ' '.join(command)
    attempts = 0
    while True:
        try:
            output = subprocess.check_output(command, shell=True,
                                             stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as err:
            attempts += 1
            if attempts == num_attempts:
                return status, err.output
        else:
            break
    # Check if the video was successfully saved.
    status = os.path.exists(output_filename)
    os.remove(tmp_filename)
    return status, 'Downloaded'


def download_clip_wrapper(row, output_dir):
    """Wrapper for parallel processing purposes."""
    output_filename = construct_video_filename(row,output_dir)

    clip_id = os.path.basename(output_filename).split('.mp4')[0]
    if os.path.exists(output_filename):
        status = tuple([clip_id, True, 'Exists'])
        return status

    downloaded, log = download_clip(row['video-id'], output_filename,
                                    row['start-time'], row['end-time'])
    status = tuple([clip_id, downloaded, log])
    return status


def parse_CSV(input_csv):
    """Returns a parsed DataFrame.

    arguments:
    ---------
    input_csv: str
        Path to CSV file containing the following columns:
          'YouTube Identifier,Start time,End time,Class label'

    returns:
    -------
    dataset: DataFrame
        Pandas with the following columns:
            'video-id', 'start-time', 'end-time'
    """
    df = pd.read_csv(input_csv)
    if 'youtube_id' in df.columns:
        columns = OrderedDict([
            ('youtube_id', 'video-id'),
            ('time_start', 'start-time'),
            ('time_end', 'end-time')])
        df.rename(columns=columns, inplace=True)
    return df


def main(input_csv, output_dir,
         trim_format='%06d', num_jobs=-1,
         drop_duplicates=False):

    #parse the dataset CSV file
    dataset = parse_CSV(input_csv)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Download all clips.
    if num_jobs == 1:
        status_lst = []
        for i, row in dataset.iterrows():
            status_lst.append(download_clip_wrapper(row, output_dir))
    else:
        status_lst = Parallel(n_jobs=num_jobs, require = 'sharedmem')(delayed(download_clip_wrapper)(
            row, output_dir) for i, row in dataset.iterrows())

    # Trim all clips
    Parallel(n_jobs = num_jobs)(delayed(trim_video)(row, output_dir, trim_format) for i,row in dataset.iterrows())

    # Save download report.
    with open('download_report.json', 'w') as fobj:
        fobj.write(json.dumps(status_lst))


if __name__ == '__main__':
    description = 'Helper script for downloading and trimming HVU videos.'
    p = argparse.ArgumentParser(description=description)
    p.add_argument('input_csv', type=str,
                   help=('CSV file containing the following format: '
                         'YouTube Identifier,Start time,End time,Class label'))
    p.add_argument('output_dir', type=str,
                   help='Output directory where videos will be saved.')
    p.add_argument('-f', '--trim-format', type=str, default='%06d',
                   help=('This will be the format for the '
                         'filename of trimmed videos: '
                         'videoid_%0xd(start_time)_%0xd(end_time).mp4'))
    p.add_argument('-n', '--num-jobs', type=int, default=12)
    p.add_argument('--drop-duplicates', type=str, default='non-existent',
                   help='Unavailable at the moment')
    main(**vars(p.parse_args()))
