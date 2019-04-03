import csv, time, datetime
from operator import itemgetter


class Dataset(object):
    def __init__(self, name, columns=None, data=None):
        self.name = name
        self.columns = columns or []
        self.data = data or []

    def populate(self, csv_path, limit=None):
        with open(csv_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for i, row in enumerate(csv_reader):
                if limit and i > limit:
                    break

                if i == 0:
                    self.columns = row
                    continue
                self.data.append(self._parse_row(row))

    def _parse_row(self, row):
        element = []
        for i, cell in enumerate(row):
            # if self.columns[i] == 'date':
            #     cell = time.mktime(datetime.datetime.strptime(cell, "%Y-%m-%d").timetuple())
            #     cell = int(cell)
            try:
                cell = float(cell)
            except ValueError:
                pass
            element.append(cell)
        return tuple(element)

    def _get_column(self, column_name, row):
        return row[self.columns.index(column_name)]

    def filter(self, column_name, function):
        new_data = []
        for row in self.data:
            if function(self._get_column(column_name, row)):
                new_data.append(row)

        return Dataset(self.name, columns=self.columns, data=new_data)

    def partition(self, group_by_columns, aggregate_column=None):
        partitioned_data = {}
        for row in self.data:
            key = []
            for column_name in group_by_columns:
                key.append(self._get_column(column_name, row))
            key = tuple(key)

            if aggregate_column:
                row = self._get_column(aggregate_column, row)

            if key in partitioned_data:
                partitioned_data[key].append(row)
            else:
                partitioned_data[key] = [row]

        return partitioned_data

    def group_by_and_aggregate(self, group_by_columns, aggregate_column, aggregation):
        partitioned_data = self.partition(group_by_columns, aggregate_column=aggregate_column)

        new_data = []
        new_columns = list(group_by_columns) + [aggregate_column]
        for key, rows in partitioned_data.items():
            result = aggregation(rows)
            element = list(key) + [result]
            element = tuple(element)
            new_data.append(element)

        return Dataset(self.name, columns=new_columns, data=new_data)

    def sort_by(self, column_name, reverse=False):
        new_data = sorted(self.data, key=itemgetter(self.columns.index(column_name)), reverse=reverse)
        return Dataset(self.name, columns=self.columns, data=new_data)

    def limit(self, n):
        return Dataset(self.name, columns=self.columns, data=self.data[:n])

    def inner_join(self, other_dataset, join_column):
        indexed_data = other_dataset.partition((join_column,))

        their_columns = list(other_dataset.columns)
        their_columns.remove(join_column)
        new_columns = self.columns + their_columns

        new_data = []
        for row in self.data:
            key = (self._get_column(join_column, row),)
            if key in indexed_data:
                their_data = indexed_data[key]
                for their_row in their_data:
                    new_row = list(row)
                    for their_column in their_columns:
                        their_column_value = other_dataset._get_column(their_column, their_row)
                        new_row.append(their_column_value)
                    new_row = tuple(new_row)
                    new_data.append(new_row)

        return Dataset(self.name, columns=new_columns, data=new_data)

    def __repr__(self):
        return str(self.data)

d = Dataset('ad_report')
d.populate('C:\\Users\\Kyle\\Repos\\tikametrics\\data\\ad_report.csv')

print(d.columns)
d = d.filter('date', lambda x: x < '2017-06-01' and x > '2017-05-01')
d = d.group_by_and_aggregate(('asin',), 'sales', sum)
print(d)
d = d.sort_by('sales', reverse=True).limit(1)
print(d, d.columns)

product_report = Dataset('product_report')
product_report.populate('C:\\Users\\Kyle\\Repos\\tikametrics\\data\\product_report.csv')

d = d.inner_join(product_report, 'asin')
print(d, d.columns)
