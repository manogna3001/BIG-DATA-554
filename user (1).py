from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieReviewCount(MRJob):

    def configure_args(self):
        super(MovieReviewCount, self).configure_args()

    def mapper(self, _, line):
        user_id, _, _, _ = line.split(',')  # Assuming the user_id is in the first column
        yield user_id, 1

    def reducer(self, user_id, counts):
        total_reviews = sum(counts)
        # Modify the output format to include a colon (":") after the user ID
        yield f"{user_id}:", total_reviews

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

if __name__ == '__main__':
    MovieReviewCount.run()