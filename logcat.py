#!/usr/bin/env python -u
#

__author__ = 'bwarminski'

import sys, argparse, logging, subprocess, errno, json, CalledProcessError
from dateutil import parser as dateparser
from time import mktime

FIRST_TOKEN = "IAmFirst"

def main(args):
    # aws logs filter-log-events --log-group-name /opt/processor/var/log/processor.info.log.json --profile prod

    nexttoken = FIRST_TOKEN

    while nexttoken:
        cmd = ['aws', 'logs', 'filter-log-events', "--log-group-name", args.log_group]
        if len(args.streams) > 0:
            cmd.append('--log-stream-names')
            cmd.extend(['"%s"' % x for x in args.streams])
        if args.begin:
            cmd.append('--start-time')
            cmd.append(str(long(mktime(args.begin.timetuple()) * 1000) + args.begin.microsecond))
        if args.end:
            cmd.append('--end-time')
            cmd.append(str(long(mktime(args.end.timetuple()) * 1000) + args.end.microsecond))
        if nexttoken != FIRST_TOKEN:
            cmd.append('--next-token')
            cmd.append(nexttoken)
        cmd.extend(args.aws)
        logging.debug("%s" % cmd)
        logging.debug("%s", args)
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (out, err) = proc.communicate()
            if proc.returncode != 0:
                sys.stderr.write(err)
                raise CalledProcessError(proc.returncode, cmd, out, err)
            response = json.loads(out)
            nexttoken = response.get('nextToken', None)
            for event in response.get('events', []):
                if args.json:
                    json_event = json.loads(event)
                    print((args.format % json_event).encode('utf-8'))
                else:
                    print((args.format % event).encode('utf-8'))
        except IOError as e:
            logging.debug("exception %s" % e)
            if e.errno == errno.EPIPE:
                sys.exit(0)
            else:
                raise e


def date(str):
    return dateparser.parse(str)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Concatenates an AWS CloudWatch Log Group or Stream",
                                     epilog="As an alternative to the commandline, params can be placed in a file, one per line, and specified on the commandline like '%(prog)s @params.conf'.",
                                     fromfile_prefix_chars='@')
    parser.add_argument("log_group",
                        help="the log group",
                        metavar="LOG-GROUP")
    parser.add_argument("-v",
                        "--verbose",
                        help="increase stderr output verbosity",
                        action="store_true")
    parser.add_argument("-j",
                        "--json",
                        help="treat the log output as JSON",
                        action="store_true")
    parser.add_argument("-s",
                        "--streams",
                        help="log streams to process",
                        nargs="+",
                        default=[])
    parser.add_argument("-b",
                        "--begin",
                        help="start date",
                        type=date)
    parser.add_argument("-e",
                        "--end",
                        help="end date",
                        type=date)
    parser.add_argument("-f",
                        "--format",
                        help='Display format. ',
                        default="%(message)s")

    parser.add_argument("--aws", help="args to pass to aws cli", nargs=argparse.REMAINDER, default=[])

    args = parser.parse_args()

    # Setup logging
    if args.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)
    try:
        main(args)
    except KeyboardInterrupt:
        logging.debug("Caught KeyboardInterrupt")
        pass

