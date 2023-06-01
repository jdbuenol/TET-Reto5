from mrjob.job import MRJob
from mrjob.step import MRStep
from sys import stdout

class MRRatingDays(MRJob):
    
    def mapper_rating(self, _, line):
        user, movie, rating, genre, date = line.split(',')
        yield date, float(rating)
    
    def reducer_rating(self, date, ratings):
        rating_sum = 0
        counter = 0
        for rating in ratings:
            rating_sum += rating
            counter += 1
        average = rating_sum / counter
        yield None, (average, date)

    def reducer_get_worst_and_best_day(self, _, gen):
        data = list(gen)
        worst_average_pair = min(data)
        best_average_pair = max(data)

        l = {}
        l["worst_average_rating"] = worst_average_pair[0]
        l["worst_average_day"] = worst_average_pair[1]
        l["best_average_rating"] = best_average_pair[0]
        l["best_average_day"] = best_average_pair[1]
        stdout.write(str(l))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_rating, reducer=self.reducer_rating),
            MRStep(reducer=self.reducer_get_worst_and_best_day)
        ]

if __name__ == '__main__':
    MRRatingDays.run()