from mrjob.job import MRJob
import re

class MRWordCount(MRJob):

    def mapper(self, _, line):
        words = re.findall(r'\b[aA-zZ]\w*\b', line)  # Match words starting with a-n or A-N
        for word in words:
            if word[0].lower() <= 'n':
                yield "a-n", 1
            else:
                yield "other", 1

    def combiner(self, category, counts):
        yield category, sum(counts)
    def reducer(self, category, counts):
        yield category, sum(counts)

if __name__ == '__main__':
    MRWordCount.run()