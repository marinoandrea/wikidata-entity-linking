import argparse

PROGRAM_DESCRIPTION = '''    
Welcome to '[WDPS 2021] - Assignment 1' CLI.

This is the entry point for our program. You can
run it by passing one or more WARC archive paths
to this script.
'''


def parse_cl_args() -> list[str]:
    """
    Parses the CLI arguments. It returns
    the list of archive paths if provided.

    Returns
    -------
    `list[str]` The list of paths provided to the program. 
    """
    parser = argparse.ArgumentParser(
        prog='wdps-assignment1',
        description=PROGRAM_DESCRIPTION)
    parser.add_argument(
        'archives',
        metavar='archive_path',
        nargs='+',
        type=str,
        help='One or multiple paths for the WARC archives you want to process.')
    args = parser.parse_args()
    return args.archives  # type: ignore
