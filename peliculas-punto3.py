from mrjob.job import MRJob
from mrjob.step import MRStep

worst_rating_average = 0
worst_rating_date = None
best_rating_average = 0
best_rating_date = None

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

class MRMostViewsDay(MRJob):
    
    def mapper_get_date(self, _, line):
        user, movie, rating, genre, date = line.split(',')
        yield date, 1
    
    def combiner_get_date(self, date, value):
        yield date, sum(value)
    
    def reducer_get_date(self, date, value):
        yield None, (sum(value), date)
    
    def reducer_find_most_views_day(self, _, date_views_pairs):
        yield "most views and date: ", max(date_views_pairs)
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_date,
                   combiner=self.combiner_get_date,
                   reducer=self.reducer_get_date),
            MRStep(reducer=self.reducer_find_most_views_day)
        ]

class MRLeastViewsDay(MRJob):
    
    def mapper_get_date(self, _, line):
        user, movie, rating, genre, date = line.split(',')
        yield date, 1
    
    def combiner_get_date(self, date, value):
        yield date, sum(value)
    
    def reducer_get_date(self, date, value):
        yield None, (sum(value), date)
    
    def reducer_find_least_views_day(self, _, date_views_pairs):
        yield "least views and date: ", min(date_views_pairs)
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_date,
                   combiner=self.combiner_get_date,
                   reducer=self.reducer_get_date),
            MRStep(reducer=self.reducer_find_least_views_day)
        ]

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

class MRRatingDays(MRJob):
    
    def mapper(self, _, line):
        user, movie, rating, genre, date = line.split(',')
        yield date, float(rating)
    
    def reducer(self, date, ratings):
        global worst_rating_average
        global worst_rating_date
        global best_rating_average
        global best_rating_date
        
        rating_sum = 0
        counter = 0
        for rating in ratings:
            rating_sum += rating
            counter += 1
        average = rating_sum / counter
        if worst_rating_date == None or best_rating_date == None:
            worst_rating_average = average
            worst_rating_date = date
            best_rating_average = average
            best_rating_date = date
        elif average > best_rating_average:
            best_rating_average = average
            best_rating_date = date
        elif average < worst_rating_average:
            worst_rating_date = date
            worst_rating_average = average

class MRWorstAndBestMoviePerGenre(MRJob):

    def mapper_movie_average(self, _, line):
        user, movie, rating, genre, date = line.split(',')
        data = {}
        data["genre"] = genre
        data["rating"] = float(rating)
        yield movie, data
    
    def reducer_movie_average(self, movie, gen):
        data = list(gen)
        rating_sum = 0
        counter = 0
        genre = data[0]["genre"]
        for row in data:
            rating_sum += row["rating"]
            counter += 1
        l = {}
        l["averageScore"] = rating_sum / counter if counter != 0 else 0
        l["movie"] = movie
        yield genre, l

    def reducer_worst_best_movie(self, genre, gen):
        data = list(gen)

        worst_average_movie = data[0]["averageScore"]
        worst_movie = data[0]["movie"]
        best_average_movie = data[0]["averageScore"]
        best_movie = data[0]["movie"]

        for movie in data:
            if movie["averageScore"] > best_average_movie:
                best_average_movie = movie["averageScore"]
                best_movie = movie["movie"]
            elif movie["averageScore"] < worst_average_movie:
                worst_average_movie = movie["averageScore"]
                worst_movie = movie["movie"]
        
        l = {}
        l["worstMovie"] = worst_movie
        l["worstMovieAverageScore"] = worst_average_movie
        l["BestMovie"] = best_movie
        l["BestMovieAverageScore"] = best_average_movie

        yield genre, l

    def steps(self):
        return [
            MRStep(mapper=self.mapper_movie_average,
                   reducer=self.reducer_movie_average),
            MRStep(reducer=self.reducer_worst_best_movie)
        ]

if __name__ == '__main__':
    MRAverageRatingUser.run()
    print()
    MRMostViewsDay.run()
    print()
    MRLeastViewsDay.run()
    print()
    MRAverageRatingMovie.run()
    print()
    MRRatingDays.run()
    print("Worst Average Score: " + str(worst_rating_average) + "  Date: " + worst_rating_date)
    print("Best Average Score: " + str(best_rating_average) + "  Date: " + best_rating_date)
    print()
    print("Best and worst movie per genre: ")
    MRWorstAndBestMoviePerGenre.run()