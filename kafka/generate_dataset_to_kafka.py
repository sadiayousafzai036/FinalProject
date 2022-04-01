import os
# Hack to be able to execute data-generator script without needing to compile it
# If you have any better idea how to achieve that, please comment
import sys
from random import randint, choice
from time import sleep

import yaml

sys.path.append(os.path.abspath(os.path.join('..', 'data-generator')))

from data_generator.model.unordered_data import UnorderedDataContainer
from data_generator.model.dataset import Dataset
from data_generator.sink.kafka_writer import KafkaWriterConfiguration


if __name__ == '__main__':
    pathname = os.path.dirname(sys.argv[0])
    configuration_file_path = os.path.join('{}/configuration.yaml'.format(os.path.abspath(pathname)))
    with open(configuration_file_path) as file:
        configuration = yaml.load(file, Loader=yaml.FullLoader)
        print('Configuration = {}'.format(configuration))

    dataset = Dataset.from_yaml(configuration)
    unordered_data_container = UnorderedDataContainer.from_yaml_with_random_distribution(configuration)

    def should_send_late_data_to_kafka():
        flags = [0] * 90 + [1] * 10
        return choice(flags)

    # give Kafka 30 seconds to start
    sleep(30)
    configuration = KafkaWriterConfiguration(configuration['kafka'])
    configuration.create_or_recreate_topics()
    output_topic_name = configuration.topics[0].name


    def get_random_duration_in_seconds():
        return randint(1, 10)

    def is_valid_log():
        flags = [True] * 1000 + [False] * 2
        return choice(flags)


    while True:
        for index, visit in enumerate(dataset.visits):
            if visit.output_log_to_the_sink():
                action = visit.generate_new_action(dataset.pages, get_random_duration_in_seconds(), is_valid_log())
                unordered_data_container.wrap_action((visit.visit_id, action),
                                                     lambda generated_action: configuration.send_message(
                                                         output_topic_name,
                                                         generated_action[0],
                                                         generated_action[1]
                                                     ))
            elif visit.is_to_close:
                dataset.reinitialize_visit(visit)

            if should_send_late_data_to_kafka():
                unordered_data_container.send_buffered_actions(
                    lambda late_action: configuration.send_message(output_topic_name,
                                                                   late_action[0], late_action[1]))
