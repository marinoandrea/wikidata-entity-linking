import argparse
from typing import List

PROGRAM_DESCRIPTION = '''    
Welcome to '[WDPS 2021] - Assignment 1' CLI.

This is the entry point for our program. You can
run it by passing one or more WARC archive paths
to this script.
'''


def parse_cl_args() -> List[str]:
    """
    Parses the CLI arguments. It returns
    the list of archive paths if provided.

    Returns
    -------
    `List[str]` The list of paths provided to the program. 
    """
    parser = argparse.ArgumentParser(
        prog='wdps-assignment1',
        description=PROGRAM_DESCRIPTION)
    parser.add_argument(
        'archive',
        nargs=1,
        type=str,
        help='WARC archive you want to process.')
    """
    NOTE(andrea): this is the interface we designed initially.
    Unfortunately it looks like the grading script needs to have
    specific CLI interface.

    parser.add_argument(
        'archives',
        metavar='archive_path',
        nargs='+',
        type=str,
        help='One or multiple paths for the WARC archives you want to process.')
    parser.add_argument(
        '-o', '--output',
        required=True,
        type=str,
        help='Output TSV file path. The file will be created if it does not exist, if it exists, it will be overwritten.')
    """
    args = parser.parse_args()
    if len(args.archive) != 1:
        raise ValueError("Please input a single WARC path.")
    return args.archive[0]  # type: ignore
