from mrjob.job import MRJob

class MRSEPerEmployee(MRJob):
    def mapper(self, _, line):
        idemp, sector, salary, year = line.split(',')
        yield idemp, sector
    
    def reducer(self, idemp, sectors):
        recordedSectors = []
        for sector in sectors:
            if not sector in recordedSectors:
                recordedSectors.append(sector)
        yield "Number of SE of employee #" + idemp, len(recordedSectors)

if __name__ == '__main__':
    MRSEPerEmployee.run()