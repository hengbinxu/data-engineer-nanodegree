from cassandra.cluster import Cluster
# from cassandra.query import dict_factory
from typing import Optional, Union, Tuple, List

class ConditionModel():
    
    def __init__(self, column_name, operator, value):
        self.column_name = column_name
        self._operator = operator.lower().strip()
        self.value = value
        self.clause = self.get_clause()

    @property
    def operator(self):
        support_operators = (
            '=', '>', '<', '>=', '<=', 'in', 'contains', 'contains key'
        )
        assert self._operator in support_operators,\
            ValueError("The operator must be in {}".format(support_operators))
        return self._operator

    @operator.setter
    def operator(self, operator):
        self._operator = operator

    def get_clause(self) -> str:
        return "{column_name} {operator} {value}".format(
            column_name=self.column_name,
            operator=self.operator,
            value='%s'
        )

    def __repr__(self) -> str:
        return self.get_clause()


class CassandraConn():
    
    def __init__(self, key_space: Optional[str]=None, *args, **kwargs):
        self.cluster = Cluster(*args, **kwargs)
        self.key_space = key_space
        self.conn_db()

    def conn_db(self):
        self.session = self.cluster.connect(self.key_space)
        # self.session.row_factory = dict_factory
        if self.key_space:
            msg = 'Connect keyspace: {}'.format(self.key_space)
        else:
            msg = 'Connect default keyspace'
        print(msg)

    def switch_key_space(self, key_space: str):
        try:
            self.session.set_keyspace(key_space)
        except Exception as e:
            raise e
        if self.key_space is None:
            old_key_space = 'default'
        else:
            old_key_space = self.key_space
        print(f'Switch keyspace from `{old_key_space}` to `{key_space}`')
        self.key_space = key_space

    def create_keyspace(self, key_space: str,
                        replication_strategy: str,
                        replication_factor: int):
        replication_setting = {
            'class': replication_strategy,
            'replication_factor': replication_factor,
        }
        stmt = """
            create keyspace if not exists {key_space}
                with replication = {replication_setting}
        """
        create_stmt = stmt.format(key_space=key_space,
                    replication_setting=replication_setting)
        try:
            self.session.execute(create_stmt)
        except Exception as e:
            raise e

    def insert_data(self, table_name: str,
                insert_data: Union[Tuple[dict], List[dict], dict], **kwargs):
        insert_stmt_template = """
            insert into {table_name} ({column_names})
            values ({insert_symbols})
        """
        if not isinstance(insert_data, (tuple, list)):
            insert_data = [insert_data, ]
        
        for data in insert_data:
            column_names = ', '.join(list(data.keys()))
            insert_symbols = ', '.join(['%s']*len(data))
            insert_stmt = insert_stmt_template.format(
                table_name=table_name,
                column_names=column_names,
                insert_symbols=insert_symbols
            )
            insert_values = tuple(data.values())
            try:
                self.session.execute(insert_stmt, insert_values, **kwargs)
            except Exception as e:
                print(f"The insert stmt occurs error:{insert_stmt}")
                print(f"Insert values:\n\t{insert_values}\n")
                raise e

    def select_query(self, table_name: str,
                    where_condition: Union[Tuple[dict], List[dict], dict],
                    select_columns: Union[tuple, list, None]=None,
                    order_by: Optional[dict]=None,
                    limit: Optional[int]=None,
                    allow_filtering=False, **execute_kwargs):
        selet_stmt_template = "select {columns} from {table_name} where {conditions}"
        if select_columns is None:
            columns = '*'
        else:
            columns = ', '.join(select_columns)
        
        if isinstance(where_condition, dict):
            where_condition = [where_condition, ]
        
        all_clause, values = list(), list()
        for condition in where_condition:
            cm = ConditionModel(
                condition['column_name'],
                condition['operator'],
                condition['value']
            )
            all_clause.append(cm.clause)
            values.append(cm.value)
        
        conditions = ' and '.join(all_clause)
        select_stmt = selet_stmt_template.format(
            columns=columns, table_name=table_name,
            conditions=conditions
        )
        if order_by is not None:
            order_by_stmt = "order by {order_by_column} {asc_or_desc}".format(
                order_by_column=order_by['order_column'],
                asc_or_desc=order_by['asc_or_desc']
            )
            select_stmt = ' '.join([select_stmt, order_by_stmt])
        
        if limit is not None:
            limit_stmt = "limit {}".format(limit)
            select_stmt = ' '.join([select_stmt, limit_stmt])
        
        if allow_filtering:
            select_stmt = ' '.join([select_stmt, 'ALLOW FILTERING'])

        try:
            result = self.session.execute(select_stmt, tuple(values), **execute_kwargs)
        except Exception as e:
            print(f"The select stmt occurs error:\n\t{select_stmt}")
            print(f"values:\n\t{values}")
            raise e
        return result

    def disconnect(self):
        self.session.shutdown()
        self.cluster.shutdown()
