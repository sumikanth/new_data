from kafka_connect import KafkaClient
import csv

class CSVToKafka:
    def __init__(self, file_loc, config):
        self.location=file_loc
        self.kc=KafkaClient(config,1) 
        
    def stream_to_kafka(self):
        count=1
        with open(self.location,"r") as csvfile:
            csvfile.seek(0)
            header_row=[]
            while True:
                if count==1:
                    header_row=csvfile.readline().replace("\n","").split(",")
                    count=count+1
                    continue
                if count>1:
                    row=csvfile.readline().replace("\n","")
                    if row=='':
                        break
                    yield {k:v for k,v in zip(header_row,row.split(","))}
    
    def push_to_kafka_topic(self, topic, partitions):
        if self.kc.create_producer():
            self.kc.create_topic(topic,partitions)
        count=0
        row_gen=self.stream_to_kafka()
        while True:
            try:
                k=next(row_gen)
                self.kc.produce_message(topic,str(count),str(k),count%partitions)
                count=count+1
            except StopIteration as e:
                self.kc.producer.flush()
                break


csvkafka=CSVToKafka("C:\\Users\\Narayanan\\repos\\new_data\\new_fol\\test_data\\tinycsv.csv",{'bootstrap.servers':'localhost:29092'})
csvkafka.push_to_kafka_topic('abcde',20)

