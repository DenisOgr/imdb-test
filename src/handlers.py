from os import path
import glob
from collections import defaultdict
from multiprocessing import Pool
from utils import clear

class RatingHandler:
    """
    RatingHandler split title.ratings.tsv in to shards
    """
    def __init__(self, file, output_dir, num_shard) -> None:
        super().__init__()
        assert num_shard > 0
        assert path.isfile(file)
        assert path.isdir(output_dir)
        assert output_dir[-1] != '/', 'output_dir should not end with \\'

        self.file = file
        self.output_dir = output_dir
        self.num_shard = num_shard

    def run(self):
        """
        Run splitting
        :return:
        """
        print("Split title ratings")
        i = 0
        with(open(self.file)) as f:
            # skip head
            f.readline()
            while True:
                line = f.readline()
                i += 1
                if i % 100 == 0:
                    clear()
                    print("Iteration: %s" % str(i))
                if not line:
                    break
                line = line.split("\t")
                id = int(line[0][2:])
                shard_id = id if id < self.num_shard else id % self.num_shard
                with open("%s/cl_%s" % (self.output_dir, shard_id), "a") as myfile:
                    myfile.write("%s %s\n" % (line[0], line[1]))

    def get(self, title_uid):
        """
        Getting resource for title_uid
        :param title_uid:
        :return: list
        """
        title_id = int(title_uid[2:])
        shard_id = title_id if title_id < self.num_shard else title_id % self.num_shard
        with open("%s/cl_%s" % (self.output_dir, shard_id)) as myfile:
            while True:
                line = myfile.readline()
                if not line:
                    raise NoRatingError()
                if title_uid == line.split()[0]:
                    return line.split()

class ActorHandler:
    """
    ActorHandler split title.principals.tsv in to shards
    """
    def __init__(self, file, output_dir, num_shard) -> None:
        super().__init__()
        assert num_shard > 0
        assert path.isfile(file)
        assert path.isdir(output_dir)
        assert output_dir[-1] != '/', 'output_dir should not end with \\'

        self.file = file
        self.output_dir = output_dir
        self.num_shard = num_shard

    def run(self, rating_handler: RatingHandler):
        """
        Run splitting
        :return:
        """
        print("Split actors")
        i = 0
        with(open(self.file, encoding="utf-8")) as f:
            # skip head
            f.readline()
            while True:
                line = f.readline()
                i += 1
                if i % 100 == 0:
                    clear()
                    print("Iteration: %s"%str(i))
                if not line:
                    break
                line = line.split("\t")
                if line[3] == 'actor':
                    actor_id = int(line[2][2:])
                    try:
                        rating = rating_handler.get(line[0])[1]
                    except NoRatingError:
                        continue
                    shard_id = actor_id if actor_id < self.num_shard else actor_id % self.num_shard
                    with open("%s/cl_%s" % (self.output_dir, shard_id), "a") as myfile:
                        myfile.write("%s %s\n" % (line[2], rating))


class CountAvgHandler:
    """
    CountAvgHandler count avg rating grouped by actor
    """
    def __init__(self, input_dir, output_dir) -> None:
        super().__init__()
        assert path.isdir(input_dir)
        assert path.isdir(output_dir)
        assert input_dir[-1] != '/', 'input_dir should not end with \\'
        assert output_dir[-1] != '/', 'output_dir should not end with \\'

        self.output_dir = output_dir
        self.input_dir = input_dir

    def run(self):
        """
        @deprecate
        Computing avg per actors
        :return:
        """
        print("Count avg per actors(single)")
        for cl in glob.glob("%s/cl_*" % self.input_dir):
            tmp = defaultdict(lambda: (0.0, 0))
            with(open(cl, encoding="utf-8")) as f:
                cl_id = cl.split("/")[-1].split("_")[-1]
                while True:
                    line = f.readline()
                    if not line:
                        break
                    line = line.split()
                    key = line[0]
                    tmp[key] = (tmp[key][0] + float(line[1]), tmp[key][1] + 1)
            with(open("%s/cl_%s" % (self.output_dir, cl_id), "w")) as f:
                for key in tmp.keys():
                    value = round(tmp[key][0] / tmp[key][1], 2)
                    f.write("%s %s\n" % (key, value))

    def run_multi(self):
        """
        Computing avg per actors (parallel style)
        :return:
        """
        print("Count avg per actors (multi)")

        files = [(cl, self.output_dir) for cl in glob.glob("%s/cl_*" % self.input_dir)]
        pool = Pool(5)
        pool.map(CountAvgHandler._run_multi_process, files)
        pool.close()


    @staticmethod
    def _run_multi_process(args):
        """
        Computing avg
        :param args:
        :return:
        """
        assert len(args) == 2
        assert path.isfile(args[0])
        assert path.isdir(args[1])
        cl = args[0]
        output_dir = args[1]

        tmp = defaultdict(lambda: (0.0, 0))
        with(open(cl, encoding="utf-8")) as f:
            cl_id = cl.split("/")[-1].split("_")[-1]
            while True:
                line = f.readline()
                if not line:
                    break
                line = line.split()
                key = line[0]
                tmp[key] = (tmp[key][0] + float(line[1]), tmp[key][1] + 1)
        with(open("%s/cl_%s" % (output_dir, cl_id), "w")) as f:
            for key in tmp.keys():
                value = round(tmp[key][0] / tmp[key][1], 2)
                f.write("%s %s\n" % (key, value))

class ComputeTopHandler:
    """
    ComputeTopHandler count top actors
    """
    def __init__(self, input_dir, names_file, num_top) -> None:
        super().__init__()
        assert path.isdir(input_dir)
        assert path.isfile(names_file)
        assert input_dir[-1] != '/', 'input_dir should not end with \\'
        assert num_top > 0

        self.names_file = names_file
        self.input_dir = input_dir
        self.num_top = num_top

        self.persons = []
        self.ratings = []

    def run(self):
        """
        Computing top
        :return:
        """
        result = []
        for cl in glob.glob("%s/cl_*" % self.input_dir):
            with(open(cl, encoding="utf-8")) as f:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    line = line.split()
                    value = (line[0], float(line[1]))
                    result.append(value)
                    if len(result) > self.num_top:
                        result.sort(key=lambda t: t[1], reverse=True)
                        result = result[:self.num_top]

        self.persons = [p[0] for p in result]
        self.ratings = [p[1] for p in result]
        del result

        with(open(self.names_file, encoding="utf-8")) as f:
            # skip head
            f.readline()
            while True:
                line = f.readline()
                if not line:
                    break
                line = line.split("\t")
                if line[0] in self.persons:
                    idx = self.persons.index(line[0])
                    self.persons[idx] = line[1]

    def print_top(self):
        """
        Print top actors
        :return:
        """
        for idx in range(self.num_top):
            print("%s-%s" % (self.persons[idx], self.ratings[idx]))


class NoRatingError(Exception):
    """
    Raised when there are any rating for film.
    """
    pass

