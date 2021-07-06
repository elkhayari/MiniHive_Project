from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse

'''
Control where the input data comes from, and where output data should go.
'''


class ExecEnv(Enum):
    LOCAL = 1  # read/write local files
    HDFS = 2  # read/write HDFS
    MOCK = 3  # read/write mock data to an in-memory file system.


'''
Switches between different execution environments and file systems.
'''


class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)

    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)


'''
Counts the number of steps / luigi tasks that we need for evaluating this query.
'''


def count_steps(raquery):
    assert (isinstance(raquery, radb.ast.Node))

    if (isinstance(raquery, radb.ast.Select) or isinstance(raquery, radb.ast.Project) or
            isinstance(raquery, radb.ast.Rename)):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception("count_steps: Cannot handle operator " + str(type(raquery)) + ".")


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    '''
    Each physical operator knows its (partial) query string.
    As a string, the value of this parameter can be searialized
    and shipped to the data node in the Hadoop cluster.
    '''
    querystring = luigi.Parameter()

    '''
    Each physical operator within a query has its own step-id.
    This is used to rename the temporary files for exhanging
    data between chained MapReduce jobs.
    '''
    step = luigi.IntParameter(default=1)

    '''
    In HDFS, we call the folders for temporary data tmp1, tmp2, ...
    In the local or mock file system, we call the files tmp1.tmp...
    '''

    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)


'''
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
'''


def task_factory(raquery, step=1, env=ExecEnv.HDFS):
    assert (isinstance(raquery, radb.ast.Node))

    if isinstance(raquery, radb.ast.Select):
        return SelectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")


class JoinTask(RelAlgQueryTask):

    def requires(self):
        #print('>>>> JOIN >>>>>>')
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Join))

        task1 = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)
        task2 = task_factory(raquery.inputs[1], step=self.step + count_steps(raquery.inputs[0]) + 1,
                             env=self.exec_environment)

        return [task1, task2]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond
        #print('> IN JOIN => ', relation, json_tuple)
        #print('condition', raquery.cond)
        #for input in raquery.inputs:
            #print('inPUT', input)
        input0 = condition.inputs[0]
        input1 = condition.inputs[1]
        # relation whitout rename
        # print(type(condition))
        if type(input0) == type(condition) and type(input1) == type(condition):
            #print('tuple', tuple)
            input00 = input0.inputs[0]
            input01 = input0.inputs[1]
            input10 = input1.inputs[0]
            input11 = input1.inputs[1]
            if str(input00) in json_tuple.keys():
                attr0 = str(input00)
            else:
                attr0 = str(input01)
            if str(input10) in json_tuple.keys():
                attr1 = str(input10)
            else:
                attr1= str(input11)
            #print(attr0, json_tuple[attr0], str(json_tuple[attr0]+str(json_tuple[attr1])))
            #print(attr1, json_tuple[attr1])
            yield (str(json_tuple[attr0]) + str(json_tuple[attr1]), (relation, tuple))
        else:
            if str(input0) in json_tuple.keys():
                #print('this', input0)
                attr = str(input0)
            else:
                #print('this one', input1)
                attr = str(input1)
            yield (json_tuple[attr], (relation, tuple))

        #print('\n')



    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        #print('\x1b[6;30;42m' + '>> REDUCER!' + '\x1b[0m')
        #print('>>> REDUCER', key)
        dic_tuple = {}
        start = 0
        values_1 = [v for v in values]
        #print('values_1')
        #for v in values_1:
            #print(v)

        for i, val1 in enumerate(values_1):
            relation1, tuple1 = val1
            json_tuple1 = json.loads(tuple1)
            #print('i :', i)
            for j, val2 in enumerate(values_1):

                relation2, tuple2 = val2
                json_tuple2 = json.loads(tuple2)
                if relation1 != relation2 and j >= start:
                    #print('j :',j)
                    #print(tuple1, tuple2)
                    for key in json_tuple1.keys():
                        dic_tuple[key] = json_tuple1[key]
                    for key in json_tuple2.keys():
                        dic_tuple[key] = json_tuple2[key]
                    new_tuple = json.dumps(dic_tuple)
                    yield 'newRelation', new_tuple


            start = i + 1

        #print('\n')


# --------------------------------------------------------------------------------------------
class SelectTask(RelAlgQueryTask):
    def requires(self):
        #print('require')
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Select))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        #print('SELECT mapper')
        #print('\x1b[6;20;42m' + '> SELECT MAPPER!' + '\x1b[0m')
        relation, tuple = line.split('\t')
        #print('tuple', tuple, type(tuple))
        json_tuple = json.loads(tuple)

        condition = radb.parse.one_statement_from_string(self.querystring).cond
        #print('condition', condition)
        is_true = False
        is_true = self.break_conditions(condition, relation, json_tuple, is_true)


        if is_true:
            #print('yieald', relation, tuple)
            yield relation, tuple
            #print('\n')
        #print('\n')

    def break_conditions(self, cond, relation, json_tuple, is_true ):
        #print('break condition', cond, relation, json_tuple)
        #print(cond, relation, json_tuple)
        #print(cond.op)
        if cond.op == 43:
            input0 = cond.inputs[0]
            input1 = cond.inputs[1]

            if type(input0) != type(radb.ast.AttrRef(None, 'a')):
                input0 = cond.inputs[1]
                input1 = cond.inputs[0]
            if input0.rel is None:
                # here if attr without rename table like age=16
                attr = relation + '.' + input0.name
                if str(json_tuple[attr]) == input1.val.replace('\'', ''):
                    is_true =  True
                else:
                    is_true = False

            else:
                #print('>> ',json_tuple[str(input0)] , input1.val.replace('\'', ''), str(json_tuple[str(input0)]) == str(input1.val.replace('\'', '')))
                if str(json_tuple[str(input0)]) == input1.val.replace('\'', ''):
                    is_true =  True
        elif cond.op == 11:
            for c in cond.inputs:
                is_true = self.break_conditions(c, relation, json_tuple, is_true)

        #else:
            #print('!!! this condition not implemented')

        return is_true




class RenameTask(RelAlgQueryTask):

    def requires(self):
        #print('require in rename')
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Rename))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)
        raquery = radb.parse.one_statement_from_string(self.querystring)
        relname = raquery.relname
        inputs = raquery.inputs[0]
        rel = inputs.rel

        yield (relation+relname, tuple.replace(rel+'.', relname+'.'))




class ProjectTask(RelAlgQueryTask):

    def requires(self):
        #print('>> Project >>')
        raquery = radb.parse.one_statement_from_string(self.querystring)
        #print(raquery.inputs[0], type(raquery.inputs[0]))
        assert (isinstance(raquery, radb.ast.Project))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        #print('MaPPer')
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        attrs = radb.parse.one_statement_from_string(self.querystring).attrs
        #print('>>> tuple', tuple)
        dic_tuple = {}
        for attr in attrs:
            #print(attr)
            if attr.rel is None:
                new_attr = relation + '.' + attr.name
            else:
                new_attr = attr.rel + '.' + attr.name
            dic_tuple[new_attr] = json_tuple[new_attr]
            #print('dic_tuple', dic_tuple)

        new_tuple = json.dumps(dic_tuple)
        yield new_tuple, new_tuple


        #print('\n')

    def reducer(self, key, values):
        ''' ...................... fill in your code below ........................'''
        #print('reducer', key)
        yield key, key

        ''' ...................... fill in your code above ........................'''


if __name__ == '__main__':
    luigi.run()
