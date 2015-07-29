#!/usr/bin/env python
#

__author__ = 'bwarminski'

import sys, argparse, logging, datetime, dateutil, subprocess, signal, errno, json, CalledProcessError, base64, zlib
from itertools import chain

class LogBuffer:
    def __init__(self, stream, shard, next_iterator, aws_args):
        self.shard = shard
        self.next_iterator = next_iterator
        self.state = "OPEN"
        self.buffer = []
        self.last_seq = None
        self.aws_args = aws_args
        self.children = None
        self.sibling = None

    def isEmpty(self):
        return len(self.buffer) == 0

    def get_more(self, type="TRIM_HORIZON"):
        # aws kinesis get-records --shard-iterator "AAAA
        if self.state is not "OPEN":
            return False
        if not self.next_iterator:
            self.next_iterator = get_shard_iterator(self.stream,self.shard,
                                           "AFTER_SEQUENCE_NUMBER" if self.last_seq else type,
                                           self.aws_args,
                                           None if not self.last_seq else self.last_seq)
            return self.get_more()

        cmd = ['aws', 'kinesis', 'get-records', '--shard-iterator', self.next_iterator]
        cmd.extend(self.aws_args)
        logging.debug("%s" % cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (out,err) = proc.communicate()
        if proc.returncode == 255 and "ExpiredIteratorException" in err:
            self.next_iterator = None
            return self.get_more(type)
        if proc.returncode != 0:
            raise CalledProcessError(proc.returncode, cmd, out, err)
        json_out = json.loads(out)
        self.buffer = list(chain.from_iterable([self.extract_buffer(x["Data"]) for x in json_out["Records"]]))
        self.last_seq = None if len(json_out["Records"]) == 0 else json_out["Records"][-1]["SequenceNumber"]
        self.next_iterator = json_out["NextShardIterator"]
        self.state = "OPEN" if self.next_iterator else "CLOSED"
        if self.next_iterator and len(self.buffer) == 0 and json_out["MillisBehindLatest"] > 0:
            return self.get_more()
        if not self.next_iterator:
            streams = describe_stream(self.stream, self.aws_args)
            self.children = [stream for stream in streams if "ParentShardId" in stream and stream["ParentShardId"] == self.shard]
            if len(self.children) > 0:
                self.state = "PARENT"
        return self.isEmpty()

    def peek(self):
        # Advance to the next record, getting more if needed
        while self.isEmpty() and self.get_more():
            continue

        if self.isEmpty():
            return None

        return self.buffer[0]

    def pop(self):
        return self.buffer.pop(0)

    def extract_buffer(self, recordString):
        logging.debug("Decoding recordString %s" % recordString)
        records = json.loads(zlib.decompress(base64.b64decode(recordString), 16+zlib.MAX_WBITS))
        return [] if not 'logEvents' in records else records['logEvents']



def describe_stream(stream, aws_args):
    # aws kinesis describe-stream --stream-name processor-log-stream
    cmd = ['aws', 'kinesis', 'describe-stream', '--stream-name', stream]
    cmd.extend(aws_args)
    logging.debug("%s" % cmd)
    response = json.loads(subprocess.check_output(cmd))
    return response["StreamDescription"]["Shards"]


def get_shard_iterator(stream, shard, type, aws_args, after_seq=None):
    # aws kinesis get-shard-iterator --stream-name processor-log-stream --shard-id shardId-000000000000 --shard-iterator-type LATEST
    cmd = ['aws', 'kinesis', 'get-shard-iterator', '--stream-name', stream, '--shard-id', shard, '--shard-iterator-type', type]
    if after_seq:
        cmd.append("--starting-sequence-number")
        cmd.append(after_seq)
    cmd.extend(aws_args)
    logging.debug("%s" % cmd)
    response = json.loads(subprocess.check_output(cmd))
    #TODO Handle HasMoreShards
    return response["ShardIterator"]




def main(args):
    # Describe all of the shards in the stream, set up a buffer & buffer state for each
    stream = args.kinesis_stream
    shards = describe_stream(stream, args.aws)
    buffers = {}
    for shard in shards:
        iter = get_shard_iterator(stream, shard['ShardId'], args.type, args.aws)
        if iter:
            logging.debug("Got iter: %s for shard %s" % (iter, shard['ShardId']))
            buffer = LogBuffer(stream, shard['ShardId'], iter, args.aws)
            buffer.get_more(args.type)
            buffers[shard['ShardId']] = buffer

    while len(buffers) > 0:
        # For each buffer:
        candidate = None

        logging.debug("Evaluating buffers")
        for (shard, buffer) in buffers.items():
            logging.debug("Trying shard %s" % shard)
            if buffer.state == 'CLOSED' and buffer.isEmpty():
                logging.debug("CLOSED and empty. Removing")
                del buffers[shard]
                continue

            # If it's PARENT and empty, add the children if absent, remove this one
            if buffer.state == 'PARENT' and buffer.isEmpty():
                logging.debug("PARENT and empty, deleting and adding children")
                del buffers[shard]
                for child in buffer.children:
                    if not child.shard in buffers:
                        logging.debug("Adding child %s" % child)
                        buffers[child.shard] = child
                candidate = None
                break

            top = buffer.peek()
            if top:
                candidate = buffer if not candidate or top['timestamp'] < candidate.peek()['timestamp'] else candidate

        # If there's a candidate, print it, pop from buffer
        if candidate:
            try:
                record = candidate.pop()
                logging.debug("Popped %s" % record)
                if args.json:
                    json_record = json.loads(record['message'])
                    print((args.format % json_record).encode('utf-8'))
                else:
                    print((args.format % record).encode('utf-8'))
            except IOError as e:
                logging.debug("exception %s" % e)
                if e.errno == errno.EPIPE:
                    sys.exit(0)
                else:
                    raise e

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Tails your CloudWatch Logs Kinesis Stream",
                                     epilog="As an alternative to the commandline, params can be placed in a file, one per line, and specified on the commandline like '%(prog)s @params.conf'.",
                                     fromfile_prefix_chars='@')
    parser.add_argument("kinesis_stream",
                        help="the kinesis stream",
                        metavar="STREAM-NAME")
    parser.add_argument("-v",
                        "--verbose",
                        help="increase stderr output verbosity",
                        action="store_true")
    parser.add_argument("-j",
                        "--json",
                        help="treat the log output as JSON",
                        action="store_true")
    parser.add_argument("-f",
                        "--format",
                        help='Display format. ',
                        default="%(message)s")
    parser.add_argument("-t",
                        "--type",
                        help="Iterator type. LATEST (default), TRIM_HORIZON",
                        default="LATEST")
    parser.add_argument("--aws", help="args to pass to aws cli (each time)", nargs=argparse.REMAINDER, default=[])

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

