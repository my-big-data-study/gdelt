import logging
import os
import re

from kafka import KafkaProducer


def init_log():
    log_format = "%(asctime)s - %(levelname)s - %(user)s[%(ip)s] - %(message)s"
    date_format = "%m/%d/%Y %H:%M:%S %p"
    logging.basicConfig(format=log_format, datefmt=date_format)


def rmdirs(root_dirpath):
    """
    delete all files and directory
    """
    if not os.path.isdir(root_dirpath):
        logging.error("{} is not directory".format(root_dirpath))
    for sub_dirpath, dirnames, filenames in os.walk(root_dirpath):
        for filename in filenames:
            os.remove(os.path.join(sub_dirpath, filename))
        for dirname in dirnames:
            rmdirs(os.pardir.join(sub_dirpath, dirname))
    os.rmdir(root_dirpath)


def send_message_to_gdelk_topics(file_list, kafka_host='localhost:9092', delete=False):
    """
    :param dir_path: the data path
    :type delete: is need to delete dirpath after send all message to kafka
    :type kafka_host: your kafka bootstrap server list
    """

    kafka_producer = KafkaProducer(bootstrap_servers=kafka_host)
    for file in file_list:
        send_message_to_gdelk_topic(file, kafka_producer)
    if delete:
        rmdirs(os.path.dirname(file_list[0]))
        logging.info("remove the base diretory %s", os.path.dirname(file_list[0]))


def send_message_to_gdelk_topic(file_path: str, producer: KafkaProducer, delete=True):
    type_name = re.match(r".+\d{14}\.(\w+)\.csv", file_path, flags=re.I).group(1)
    topic_name = str.format("gdelk_{}", type_name)
    with open(file_path, 'r') as csv_file:
        for line in csv_file:
            producer.send(topic=topic_name, value=str.encode(line))
    producer.flush()
    if delete:
        logging.info("remove file %s", file_path)
        os.remove(file_path)


# if __name__ == '__main__':
#     producer = KafkaProducer(bootstrap_servers='kafka:9092')
#     send_message_to_gdelk_topic("/data/20200527170000/20200527170000.export.CSV", producer, False)
#     # send_message_to_gdelk_topic("./data/20200527161500/20200527161500.export.CSV", producer)
