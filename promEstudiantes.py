from mrjob.job import MRJob
from mrjob.job import MRStep
class EstudiantePromedio(MRJob):
    def mapper(self, _, line):
        if line.split('|')[86]=='punt_lenguaje':
            pass
        else:
            p_1=float(line.split('|')[86].replace(',','.'))
            p_2=float(line.split('|')[87].replace(',','.'))
            p_3=float(line.split('|')[88].replace(',','.'))
            p_4=float(line.split('|')[89].replace(',','.'))
            p_5=float(line.split('|')[90].replace(',','.'))
            p_6=float(line.split('|')[91].replace(',','.'))
            p_7=float(line.split('|')[92].replace(',','.'))
            p_8=float(line.split('|')[93].replace(',','.'))
            total=(p_1+p_2+p_3+p_4+p_5+p_6+p_7+p_8)/8
            yield  line.split('|')[4],total

    def reducer1(self,key,valores):
        yield key,sum(valores)

    def reducer2(self,key,valores):
        yield None,(sum(valores),key)
  
    def reducer3(self,_,valores):
        max_val=0
        key_max=''
        for valor,palabra in  valores:      
            if valor>max_val:
                max_val=valor
                key_max=palabra
        yield key_max,max_val
    
  
    def steps(self):
        return [
              MRStep(mapper = self.mapper,
              reducer = self.reducer1)
              ,MRStep(reducer = self.reducer2)
               ,MRStep(reducer = self.reducer3)
            ]   

if __name__ == '__main__':
    EstudiantePromedio.run()  
