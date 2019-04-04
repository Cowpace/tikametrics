from unittest import TestCase
from dataset import Dataset


class Tests(TestCase):
    def setUp(self):
        self.dataset = Dataset('test')
        self.dataset.populate('ad_report_test.csv')

    def test_dataset(self):
        actual = self.dataset.columns
        expected = ['date','impressions','clicks','sales','ad_spend','keyword_id','asin']
        self.assertEqual(actual, expected)

        actual = self.dataset.data
        expected = [
            ('2017-06-19', 4451.0, 1006.0, 608.0, 24.87, 'KEYWORDID1', 'ASIN1'),
            ('2017-06-18', 5283.0, 3237.0, 1233.0, 85.06, 'KEYWORDID1', 'ASIN1'),
            ('2017-06-17', 0.0, 0.0, 0.0, 21.77, 'KEYWORDID1', 'ASIN2'),
            ('2017-06-16', 0.0, 0.0, 0.0, 68.99, 'KEYWORDID1', 'ASIN2'),
            ('2017-06-15', 0.0, 0.0, 0.0, 56.03, 'KEYWORDID1', 'ASIN2'),
            ('2017-06-14', 9104.0, 5674.0, 4635.0, 147.39, 'KEYWORDID1', 'ASIN1'),
            ('2017-06-13', 0.0, 0.0, 0.0, 59.63, 'KEYWORDID1', 'ASIN1'),
            ('2017-06-12', 4713.0, 4646.0, 2169.0, 82.34, 'KEYWORDID2', 'ASIN1'),
            ('2017-06-11', 2835.0, 2671.0, 2118.0, 65.1, 'KEYWORDID2', 'ASIN1'),
            ('2017-06-10', 0.0, 0.0, 0.0, 64.19, 'KEYWORDID2', 'ASIN1'),
            ('2017-06-09', 3692.0, 113.0, 71.0, 48.4, 'KEYWORDID2', 'ASIN1'),
            ('2017-06-08', 0.0, 0.0, 0.0, 40.31, 'KEYWORDID2', 'ASIN1'),
            ('2017-06-07', 1256.0, 869.0, 805.0, 37.64, 'KEYWORDID2', 'ASIN1'),
            ('2017-06-06', 5498.0, 0.0, 0.0, 60.57, 'KEYWORDID2', 'ASIN1')
        ]

        self.assertListEqual(actual, expected)

    def test_filter(self):
        new_dataset = self.dataset.filter('impressions', lambda x: x == 0)

        actual = new_dataset.data
        expected = [
            ('2017-06-17', 0.0, 0.0, 0.0, 21.77, 'KEYWORDID1', 'ASIN2'),
            ('2017-06-16', 0.0, 0.0, 0.0, 68.99, 'KEYWORDID1', 'ASIN2'),
            ('2017-06-15', 0.0, 0.0, 0.0, 56.03, 'KEYWORDID1', 'ASIN2'),
            ('2017-06-13', 0.0, 0.0, 0.0, 59.63, 'KEYWORDID1', 'ASIN1'),
            ('2017-06-10', 0.0, 0.0, 0.0, 64.19, 'KEYWORDID2', 'ASIN1'),
            ('2017-06-08', 0.0, 0.0, 0.0, 40.31, 'KEYWORDID2', 'ASIN1'),
        ]

        self.assertEqual(actual, expected)

    def test_partition(self):
        actual = self.dataset._partition(('keyword_id', 'asin'))
        expected = {
            ('KEYWORDID1', 'ASIN1'): [
                ('2017-06-19', 4451.0, 1006.0, 608.0, 24.87, 'KEYWORDID1', 'ASIN1'),
                ('2017-06-18', 5283.0, 3237.0, 1233.0, 85.06, 'KEYWORDID1', 'ASIN1'),
                ('2017-06-14', 9104.0, 5674.0, 4635.0, 147.39, 'KEYWORDID1', 'ASIN1'),
                ('2017-06-13', 0.0, 0.0, 0.0, 59.63, 'KEYWORDID1', 'ASIN1')
            ],
            ('KEYWORDID1', 'ASIN2'): [
                ('2017-06-17', 0.0, 0.0, 0.0, 21.77, 'KEYWORDID1', 'ASIN2'),
                ('2017-06-16', 0.0, 0.0, 0.0, 68.99, 'KEYWORDID1', 'ASIN2'),
                ('2017-06-15', 0.0, 0.0, 0.0, 56.03, 'KEYWORDID1', 'ASIN2')
            ],
            ('KEYWORDID2', 'ASIN1'): [
                ('2017-06-12', 4713.0, 4646.0, 2169.0, 82.34, 'KEYWORDID2', 'ASIN1'),
                ('2017-06-11', 2835.0, 2671.0, 2118.0, 65.1, 'KEYWORDID2', 'ASIN1'),
                ('2017-06-10', 0.0, 0.0, 0.0, 64.19, 'KEYWORDID2', 'ASIN1'),
                ('2017-06-09', 3692.0, 113.0, 71.0, 48.4, 'KEYWORDID2', 'ASIN1'),
                ('2017-06-08', 0.0, 0.0, 0.0, 40.31, 'KEYWORDID2', 'ASIN1'),
                ('2017-06-07', 1256.0, 869.0, 805.0, 37.64, 'KEYWORDID2', 'ASIN1'),
                ('2017-06-06', 5498.0, 0.0, 0.0, 60.57, 'KEYWORDID2', 'ASIN1')
            ]
        }

        self.assertDictEqual(actual, expected)

    def test_partition_with_aggregate_column(self):
        actual = self.dataset._partition(('keyword_id', 'asin'), aggregate_column='clicks')
        expected = {
            ('KEYWORDID1', 'ASIN1'): [1006.0, 3237.0, 5674.0, 0.0],
            ('KEYWORDID1', 'ASIN2'): [0.0, 0.0, 0.0],
            ('KEYWORDID2', 'ASIN1'): [4646.0, 2671.0, 0.0, 113.0, 0.0, 869.0, 0.0]
        }

        self.assertDictEqual(actual, expected)

    def test_group_by_and_aggregate(self):
        actual = self.dataset.group_by_and_aggregate(('keyword_id', 'asin'), 'clicks', max)
        expected_data = [('KEYWORDID1', 'ASIN1', 5674.0), ('KEYWORDID1', 'ASIN2', 0.0), ('KEYWORDID2', 'ASIN1', 4646.0)]
        expected_columns = ['keyword_id', 'asin', 'clicks']

        self.assertListEqual(actual.columns, expected_columns)
        self.assertListEqual(actual.data, expected_data)

    def test_inner_join(self):
        dataset = Dataset(
            'dataset',
            columns=['keyword_id', 'asin', 'clicks'],
            data=[
                ('KEYWORDID1', 'ASIN1', 5674.0),
                ('KEYWORDID1', 'ASIN2', 0.0),
                ('KEYWORDID2', 'ASIN1', 4646.0)
            ]
        )
        other_dataset = Dataset(
            'other_dc',
            columns=['keyword_id', 'name'],
            data=[
                ('KEYWORDID1', 'a keyword'),
                ('KEYWORDID2', 'another keyword'),
                ('KEYWORDID3', 'another similar keyword')
            ]
        )

        actual = dataset.inner_join(other_dataset, 'keyword_id')

        expected_columns = ['keyword_id', 'asin', 'clicks', 'name']
        expected_data = [
            ('KEYWORDID1', 'ASIN1', 5674.0, 'a keyword'),
            ('KEYWORDID1', 'ASIN2', 0.0, 'a keyword'),
            ('KEYWORDID2', 'ASIN1', 4646.0, 'another keyword')
        ]

        self.assertListEqual(actual.columns, expected_columns)
        self.assertListEqual(actual.data, expected_data)

    def test_inner_join_duplicates(self):
        dataset = Dataset(
            'dataset',
            columns=['keyword_id', 'asin', 'clicks'],
            data=[
                ('KEYWORDID1', 'ASIN1', 5674.0),
                ('KEYWORDID1', 'ASIN2', 0.0),
                ('KEYWORDID2', 'ASIN1', 4646.0)
            ]
        )
        other_dataset = Dataset(
            'other_dc',
            columns=['keyword_id', 'name'],
            data=[
                ('KEYWORDID1', 'a keyword'),
                ('KEYWORDID1', 'another keyword'),
                ('KEYWORDID3', 'another similar keyword')
            ]
        )

        actual = dataset.inner_join(other_dataset, 'keyword_id')

        expected_columns = ['keyword_id', 'asin', 'clicks', 'name']
        expected_data = [
            ('KEYWORDID1', 'ASIN1', 5674.0, 'a keyword'),
            ('KEYWORDID1', 'ASIN1', 5674.0, 'another keyword'),
            ('KEYWORDID1', 'ASIN2', 0.0, 'a keyword'),
            ('KEYWORDID1', 'ASIN2', 0.0, 'another keyword'),
        ]

        self.assertListEqual(actual.columns, expected_columns)
        self.assertListEqual(actual.data, expected_data)
