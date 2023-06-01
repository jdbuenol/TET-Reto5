from mrjob.job import MRJob

class MRAverageRatingMovie(MRJob):

    def mapper(self, _, line):
        user, movie, rating, genre, date = line.split(',')
        yield movie, float(rating)

    def reducer(self, movie, ratings):
        rating_sum = 0
        counter = 0
        for rating in ratings:
            rating_sum += rating
            counter += 1
        l = {}
        l["averageScore"] = rating_sum / counter
        l["totalViewers"] = counter
        yield "Movie #" + movie, l


if __name__ == '__main__':
    MRAverageRatingMovie.run()