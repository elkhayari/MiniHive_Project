import radb
import radb.ast
import radb.parse
import sqlparse
from sqlparse.tokens import Keyword, DML, Wildcard, Punctuation, Comparison, Keyword, Comparison, Literal, Name, Token


class my_tokens(dict):
    # __init__ function
    def __init__(self):
        self = dict()

        # Function to add key:value
    def add(self, key, value):
        self[key] = value

def translate(stmt):
    tokens = [token for token in stmt if not token.is_whitespace]
    tokensDict = my_tokens()

    for i, token in enumerate(tokens):
        if str(token) == 'select':
            tokensDict.add('select', token)
        elif str(token) == 'distinct':
            tokensDict.add('distinct', token)
        elif str(tokens[i-1]) == 'distinct':
            tokensDict.add('attrs', token)
        elif str(token) == 'from':
            tokensDict.add('from', token)
        elif str(tokens[i-1]) == 'from':
            tokensDict.add('inputs', token)
        elif str(tokens[i-2]) == 'from':
            tokensDict.add('cond', token)
        else:
            print('this token', token, 'does not exists')
    if 'cond' not in tokensDict.keys():
        tokensDict.add('cond', None)

    if tokensDict['cond'] is None:
        cond = None
    else:
        cond = setCondition(tokensDict['cond'])

    if tokensDict['attrs'].ttype is Wildcard:
        input = setInputs(tokensDict)

        result = setSelect(cond, input, tokensDict)
    elif tokensDict['cond'] is not None:
        attrs = setAttrs(tokensDict['attrs'])
        input = setInputs(tokensDict)
        inputs = setSelect(cond, input, tokensDict)

        result = setProject(attrs, inputs)
    else:
        attrs = setAttrs(tokensDict['attrs'])
        inputs = setInputs(tokensDict)
        result = setProject(attrs, inputs)

    return result


def setSelect(cond, input, tokensDict):
    if tokensDict['cond'] is None:
        select = input
    else:
        cond = setCondition(tokensDict['cond'])
        select = radb.ast.Select(cond, input)
    return select


def setCross(left, right):
    return radb.ast.Cross(left, right)

def setInputs(tokensDict):
    relations = []
    input_tokens = [token for token in tokensDict['inputs'].tokens if
                    not token.is_whitespace and token.ttype is not Punctuation]
    if len(input_tokens) > 1 and input_tokens[0].ttype is not Name:
        for t in input_tokens:
            if t.is_group and len(t.tokens) > 1:
                sub_input_Tokens = [st for st in t.tokens if not st.is_whitespace and st.ttype is not Punctuation]
                relations.append([sub_input_Tokens[0], sub_input_Tokens[1]])
            else:
                relations.append(t.tokens[0])

    elif len(input_tokens) > 1 and input_tokens[0].ttype is Name:
        relations.append([input_tokens[0], input_tokens[1]])

    else:
        relations.append(input_tokens[0])

    if len(relations) > 1:
       for index, i in enumerate(relations):
           try:
               if i.ttype is Name:
                   isList = False
           except:
               isList = True

           if isList:
               if index == 0:
                   table = radb.ast.RelRef(str(i[0]))
                   left_cross = radb.ast.Rename(str(i[1]), None, table)
               else:
                   table = radb.ast.RelRef(str(i[0]))
                   right = radb.ast.Rename(str(i[1]), None, table)
                   left_cross = setCross(left_cross, right)

           else:
               if index == 0:
                   left_cross = radb.ast.RelRef(str(i))
               else:
                   right = radb.ast.RelRef(str(i))
                   left_cross = setCross(left_cross, right)

       return left_cross

    else:
        try:
          if relations[0].ttype is Name:
                isList = False
        except:
            isList = True

        if isList:
            for t in relations[0]:
                if t.ttype is Name:
                    table = radb.ast.RelRef(str(t))
                else:
                    relname = t
            input = radb.ast.Rename(str(relname), None, table)
        else:
            input = radb.ast.RelRef(str(relations[0]))
        return input


def setCondition(token):
    condition_tokens = [token for token in token.tokens if not token.is_whitespace and token.ttype is not Punctuation and str(token) != 'where']
    conditions = []
    for token in condition_tokens:

        if token.is_group:
            subCond_tokens = [st for st in token if not st.is_whitespace]
            for i, t in enumerate(subCond_tokens):
                if t.ttype is None:
                    if len(t.tokens) > 1:
                        attr = radb.ast.AttrRef(str(t.tokens[0]), str(t.tokens[2]))
                    else:
                        attr = radb.ast.AttrRef(None, str(t))
                elif t.ttype is Comparison:
                    operation = t
                elif t.ttype is Literal.Number.Integer:
                    attr = radb.ast.RANumber(str(t))
                elif t.ttype is Literal.String.Single:
                    attr = radb.ast.RAString(str(t))
                else:
                    print('Error')

                if i == 0:
                    left = attr
                elif i == 1:
                    op = operation
                else:
                    right = attr
            conditions.append(radb.ast.ValExprBinaryOp(left, radb.ast.sym.EQ, right))

        else:
            if str(token).upper() == 'AND':
                conditions.append(radb.ast.sym.AND)

    if len(conditions) >= 4:
        for i, val in enumerate(conditions):
            b = 2 *i +1
            c = 2 * (i + 1)
            if i == 0:
                cond = radb.ast.ValExprBinaryOp(val, conditions[b], conditions[c])
            elif  c <= len(conditions):
                cond = radb.ast.ValExprBinaryOp(cond, conditions[b], conditions[c])

            else:
                pass


    else:
        cond = conditions[0]
        if len(conditions) == 1:
            cond = conditions[0]
        if len(conditions) == 3:
            cond = radb.ast.ValExprBinaryOp(conditions[0], conditions[1], conditions[2])
        print('\n')


    return cond


def setProject(attrs, inputs):
    return radb.ast.Project(attrs, inputs)

def setAttrs(token):
    tokens = [t for t in token.tokens if not t.is_whitespace and t.ttype is not Punctuation]
    attrs = []
    if len(tokens) > 1:
        for a in tokens:
            if a.is_group:
                sub_tokens = [t for t in a.tokens if not t.is_whitespace and t.ttype is not Punctuation]
                if len(sub_tokens) > 1:
                    attrs.append(radb.ast.AttrRef(str(sub_tokens[0]), str(sub_tokens[1])))
                else:
                    attrs.append(radb.ast.AttrRef(None, str(sub_tokens[0])))
            else:
                attrs.append(radb.ast.AttrRef(str(tokens[0]), str(tokens[1])))
                break
    else:
        attrs.append(radb.ast.AttrRef(None, str(tokens[0])))

    return attrs
