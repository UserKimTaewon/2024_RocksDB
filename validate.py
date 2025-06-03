import json
import itertools
def parse(x):
    a,b,c=map(str.strip,x.split(','))
    return int(a),int(b),c
def parse_line(x):
    return {parse(k):v for k,v in x.items()}

def parse_file(f):
    return tuple(map(parse_line,map(json.loads,f)))

def assert_eq(i1,i2):
    for i,j in itertools.zip_longest(i1,i2):
        if i!=j:
            print(i)
            print(j)
            raise Exception('Not Equal')

def head(it):
    return next(iter(it))
def line_count(f):
    return sum(1 for _ in f)

def sorted_log(x):
    return sorted(x,key=lambda x:head(x.keys()))

if __name__=='__main__':
    with open(r"logfiles/log_1.jsonl") as f:
        orig_log=parse_file(f)
    with open(r"test_db_dump.jsonl") as f:
        dump_log=parse_file(f)
    with open(r"test_db_dump_before_chrono_break.jsonl") as f:
        before_break_log=parse_file(f)
        before_break=len(before_break_log)
    with open(r"test_db_dump_right_after_chrono_break.jsonl") as f:
        after_break_log=parse_file(f)
        after_break=len(after_break_log)
    chrono_diff=head(orig_log[before_break].keys())[1]-head(dump_log[after_break].keys())[1]
    
    orig_log=sorted_log(orig_log)
    dump_log=sorted_log(dump_log)
    print('testing dump before break...')
    assert_eq(sorted_log(before_break_log),orig_log[:before_break])
    print('testing dump after break...')
    assert_eq(orig_log[:after_break],dump_log[:after_break])
    print('testing dump after write...')
    assert_eq(orig_log[before_break:],
              tuple({(a,b+chrono_diff,c):v for (a,b,c),v in x.items()} for x in
                    dump_log[after_break:]))
    print('Dump Correct')
