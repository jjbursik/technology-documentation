"""

Parameters
----------


"""
import argparse
import concurrent.futures
from elasticsearch import Elasticsearch
from pymongo import MongoClient

failed_result = []
lineage_data_count = 0


class LineageValidation(object):
    def __init__(self):
        global lineage_data_count
        mongo_client = MongoClient(args.mongo_uri)
        db = mongo_client['dataportal']
        collection = db['lineagerelations']
        query = {"active": True}
        lineage_data_count = collection.count(query)
        print("lineage objects being processed: %s" % lineage_data_count)
        lineage_data = collection.find(query).limit(100)
        self.count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.lookup_es_entity, entity) for entity in lineage_data]

    def lookup_es_entity(self, entity):
        lineage_id = entity['_id']
        print("processing lineageId: %s" % lineage_id)
        source_index = entity['source']['sourceId']
        source_type = entity['source']['entityType']
        source_entity_id = entity['source']['entityId']
        print("source -> index: %s type: %s entityId: %s" % (source_index, source_type, source_entity_id))
        destination_index = entity['destination']['sourceId']
        destination_type = entity['destination']['entityType']
        destination_entity_id = entity['destination']['entityId']
        print("destination -> index: %s type: %s entityId: %s" % (destination_index, destination_type, destination_entity_id))
        source_query = {
            "query": {
                "ids": {
                    "values": [source_entity_id]
                }
            }
        }
        self.validate_source_lineage(source_index, source_query, destination_entity_id)
        destination_query = {
            "query": {
                "ids": {
                    "values": [destination_entity_id]
                }
            }
        }
        self.validate_destination_lineage(destination_index, destination_query, source_entity_id)
        self.count += 1
        print("processing %s / %s" % (self.count, self.lineage_data_count))

    @staticmethod
    def validate_source_lineage(index, query, destination_id):
        es_client = Elasticsearch(args.elastic_nodes.split(','), sniff_on_start=True, timeout=300)
        result = es_client.search(index=index, body=query)
        if result['hits']['total'] > 1:
            raise Exception("Found too many results")
        else:
            for hit in result['hits']['hits']:
                _id = hit['_id']
                source_ids = [d['entityId'] for d in hit['_source']['downstream']]
                if destination_id in source_ids:
                    print("source validation passed!")
                else:
                    print("source validation failed!")
                    failed_result.append("destinationId: %s not in source: %s" % (destination_id, _id))

    @staticmethod
    def validate_destination_lineage(index, query, source_id):
        es_client = Elasticsearch(args.elastic_nodes.split(','), sniff_on_start=True, timeout=300)
        result = es_client.search(index=index, body=query)
        if result['hits']['total'] > 1:
            raise Exception("Found too many results")
        else:
            for hit in result['hits']['hits']:
                _id = hit['_id']
                destination_ids = [d['entityId'] for d in hit['_source']['upstream']]
                if source_id in destination_ids:
                    print("destination validation passed!")
                else:
                    print("destination validation failed!")
                    failed_result.append("sourceId: %s not in destination: %s" % (source_id, _id))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Metadata Index Creation Monitor')
    parser.add_argument('-es', '--elastic-nodes', help='Elastic nodes to connect too', required=True)
    parser.add_argument('-m', '--mongo-uri', help='Mongo URI', required=True)
    args = parser.parse_args()
    LineageValidation()
    total_errors = len(failed_result)
    print("Total Errors: %s out of %s" % (total_errors, lineage_data_count))
    if total_errors > 0:
        for error in failed_result:
            print(error)
