from mrjob.job import MRJob

class MRAverageRatingUser(MRJob):

    def mapper(self, _, line):
        user, movie, rating, genre, date = line.split(',')
        yield user, float(rating)

    def reducer(self, user, ratings):
        rating_sum = 0
        counter = 0
        for rating in ratings:
            rating_sum += rating
            counter += 1
        l = {}
        l["averageScore"] = rating_sum / counter
        l["totalMovies"] = counter
        yield "User #" + user, l

if __name__ == '__main__':
    MRAverageRatingUser.run()