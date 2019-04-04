import os
from dataset import Dataset


class DatasetAPI(object):
    """To be honest I wasnt sure if the API mentioned in the readme meant a RESTful API or
    an API represented by the Dataset object, in either case, this is an API spec that could be
    wrapped by a service"""
    AGGREGATION_NAME_TO_FUNCTION = {
        'sum': sum,
        'max': max,
        'min': min,
        'average': lambda x: sum(x) / len(x) if x else None
    }

    def _parse_operations(self, operation_data, datasets):
        """Parse the operation component of the dictionary"""
        dataset_name = operation_data['dataset']
        operation_name = operation_data['operation_name']
        operation_args = operation_data['operation_args']

        dataset = datasets[dataset_name]

        # tbh this isnt the greatest solution, and there should be a more formal framework and system for this,
        # but this is the only way I can think of to support all of these operations
        if operation_name == 'equals_filter':
            column_name = operation_args['column_name']
            value = operation_args['value']
            dataset = dataset.equals_filter(column_name, value)
        elif operation_name == 'between_filter':
            column_name = operation_args['column_name']
            start = operation_args['start']
            end = operation_args['end']
            dataset = dataset.between_filter(column_name, start, end)
        elif operation_name == 'group_by_and_aggregate':
            group_by_columns = operation_args['group_by_columns']
            aggregate_column = operation_args['aggregate_column']
            aggregation = operation_args['aggregation']
            aggregation = self.AGGREGATION_NAME_TO_FUNCTION[aggregation]
            dataset = dataset.group_by_and_aggregate(group_by_columns, aggregate_column, aggregation)
        elif operation_name == 'sort_by':
            column_name = operation_args['column_name']
            dataset = dataset.sort_by(column_name)
        elif operation_name == 'sort_by_desc':
            column_name = operation_args['column_name']
            dataset = dataset.sort_by(column_name, reverse=True)
        elif operation_name == 'limit':
            value = operation_args['value']
            dataset = dataset.limit(value)
        elif operation_name == 'inner_join':
            column_name = operation_args['column_name']
            other_dataset = operation_args['other_dataset']
            dataset = dataset.inner_join(datasets[other_dataset], column_name)
        else:
            raise ValueError('Operation name {} is not supported'.format(operation_name))

        return dataset

    def handle_request(self, payload):
        """Handle a request with the given API:

        {
          "datasets": {
            <dataset_name>: <csv_path>
          },
          "operations": [
            {
              "dataset": <dataset_name to perform this operation on>
              "operation_name": <the name of the operation to perform>
              "operation_args": {
                <argument name as specified in the dataset object>: <value>
              }
            }
          ]
          "return": [
            <name of dataset to return>
          ]

        Args:
            payload (dict): a dictionary with the above format

        Returns:
            dict: a mapping of the dataset name to its resulting value
        """
        datasets = {}
        for dataset_name, csv_path in payload['datasets'].items():
            datasets[dataset_name] = Dataset(dataset_name).populate(csv_path)

        for operation_data in payload['operations']:
            dataset_name = operation_data['dataset']
            datasets[dataset_name] = self._parse_operations(operation_data, datasets)

        return {
            datasets[dataset_name].name: {
                'columns': datasets[dataset_name].columns,
                'data': datasets[dataset_name].data
            }
            for dataset_name in payload['return']
        }

if __name__ == '__main__':
    api = DatasetAPI()

    request = {
        'datasets': {
            'ad_report': os.path.join('data', 'ad_report.csv'),
            'product_report': os.path.join('data', 'product_report.csv')
        },
        'operations': [
            {
                'dataset': 'ad_report',
                'operation_name': 'between_filter',
                'operation_args': {
                    'column_name': 'date',
                    'start': '2017-05-01',
                    'end': '2017-06-01'
                }
            },
            {
                'dataset': 'ad_report',
                'operation_name': 'group_by_and_aggregate',
                'operation_args': {
                    'group_by_columns': ['asin'],
                    'aggregate_column': 'sales',
                    'aggregation': 'sum'
                }
            },
            {
                'dataset': 'ad_report',
                'operation_name': 'sort_by_desc',
                'operation_args': {
                    'column_name': 'sales'
                }
            },
            {
                'dataset': 'ad_report',
                'operation_name': 'limit',
                'operation_args': {
                    'value': 1
                }
            },
            {
                'dataset': 'ad_report',
                'operation_name': 'inner_join',
                'operation_args': {
                    'column_name': 'asin',
                    'other_dataset': 'product_report'
                }
            },
        ],
        'return': [
            'ad_report'
        ]
    }

    print(api.handle_request(request))
