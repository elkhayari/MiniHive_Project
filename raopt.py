import radb
import radb.ast
import radb.parse

RelRef = radb.ast.RelRef('')
Select = radb.ast.Select(radb.ast.ValExpr(), radb.ast.RelRef(''))
Project = radb.ast.Project([radb.ast.AttrRef(None, 'name')], Select)
Cross = radb.ast.Cross(Select, Select)
Rename = radb.ast.Rename('E', None, radb.ast.RelRef('Eats'))

RAString = radb.ast.RAString('')
AttrRef = radb.ast.AttrRef('A', 'Person')
global_conditions = []
select_conditions = []
global_inputs = []

def rule_break_up_selections(stmt):
    print('--------- break selections ---------------')
    if type(stmt) == type(Select):
        conditions = stmt.cond
        inputs = stmt.inputs
        print('>conditions', conditions, stmt)
        return set_selection(conditions, inputs)

    elif type(stmt) == type(Project):
        print('Project')
        attrs = stmt.attrs
        inputs = stmt.inputs
        input = rule_break_up_selections(inputs[0])
        project = set_project(attrs, input)

        return project

    elif type(stmt) == type(Cross):
        inputs = stmt.inputs
        for i, input in enumerate(inputs):
           if i == 0:
               left_c =  rule_break_up_selections(input)
           else:
               left_c = set_cross(left_c, rule_break_up_selections(input))

        return left_c

    else:
        return stmt

def rule_push_down_selections(stmt, dd):
    print('\n')
    print('-_-_-_-_-_  Push Down Selections _-_-_-_-_-_-\n', stmt)
    print('\n')
    print(type(stmt))
    global_conditions.clear()
    print('\n global inputs', global_inputs)
    if type(stmt) == type(Select):
        conds = stmt.cond
        global_conditions.append(conds)
        inputs = stmt.inputs
        newstmt = inputs[0]

        if type(newstmt) == type(RelRef):
            global_inputs.append(newstmt)
        elif type(newstmt) == type(Rename):
            global_inputs.append(newstmt)
        elif type(newstmt) == type(Cross):
            inputs = break_cross(newstmt)
        else:
            while type(newstmt) == type(Select):
                print('>> in while', newstmt)
                conds = newstmt.cond
                global_conditions.append(conds)
                inputs = newstmt.inputs
                newstmt = inputs[0]
                if type(newstmt) == type(Cross):
                    inputs = break_cross(newstmt)

                elif type(newstmt) == type(RelRef):
                    inputs = break_cross(newstmt)

                else:
                    print('# new stmt', type(newstmt))
                    print('Not Implemeted yet')

                print('\n')

    elif type(stmt) == type(Project):
        attrs = stmt.attrs
        inputs = stmt.inputs[0]
        return radb.ast.Project(attrs, rule_push_down_selections(inputs, dd))

    elif type(stmt) == type(Cross):
        print(stmt.inputs)
        inputs = break_cross(stmt)

    elif type(stmt) == type(RelRef):
        return stmt
    else:
        print(stmt, type(stmt))
        return stmt
        print('not implemented yes')

    print('\n global inputs', global_inputs)
    for i, input in enumerate(global_inputs):
        if type(input) == type(Rename):
            select = input
            for cond in reversed(global_conditions):
                if cond.op == 11:
                    continue
                if type(cond.inputs[0]) != type(AttrRef):
                    if cond.inputs[1].rel is None and cond.inputs[1].name in dd[input.inputs[0].rel].keys():
                        select = radb.ast.Select(cond, input)
                        global_conditions.remove(cond)
                        break
                elif type(cond.inputs[1]) != type(AttrRef):
                    if cond.inputs[0].rel is None and cond.inputs[0].name in dd[input.inputs[0].rel].keys():
                        select = radb.ast.Select(cond, input)
                        global_conditions.remove(cond)
                        break
                    else:
                        print(input)
                        if input.relname == cond.inputs[0].rel and type(cond.inputs[1]) != type(AttrRef):
                            select = radb.ast.Select(cond, input)
                            global_conditions.remove(cond)
                            break

                else:
                    select = input

            if i == 0:
                left_cross = select
            else:
                left_cross = set_cross(left_cross, select)

        else:
            print('>>> normal relation ')
            for cond in reversed(global_conditions):
                print('cond', cond, cond.op)
                if cond.op == 11:
                    continue
                if type(cond.inputs[0]) != type(AttrRef):
                    if cond.inputs[1].rel is None and cond.inputs[1].name in dd[input.rel].keys():
                        input = radb.ast.Select(cond, input)
                        global_conditions.remove(cond)
                        break
                    else:
                        input = input
                elif type(cond.inputs[1]) != type(AttrRef):
                    if cond.inputs[0].rel == str(input) and cond.inputs[0].name in dd[input.rel].keys():
                        input = radb.ast.Select(cond, input)
                        global_conditions.remove(cond)
                        break
                    elif cond.inputs[0].rel == str(input) and cond.inputs[0].name in dd[input.rel].keys():
                        input = radb.ast.Select(cond, input)
                        global_conditions.remove(cond)
                        break
                    elif cond.inputs[0].rel is None and cond.inputs[0].name in dd[input.rel].keys():
                        input = radb.ast.Select(cond, input)
                        global_conditions.remove(cond)
                        break
                    else:
                        input = input

                else:
                    input = input

            cross_relations = []
            if i == 0:
                left_cross = input
            else:
                left_cross = set_cross(left_cross, input)
                print('left_cross', left_cross)
                if len(global_conditions) > 0:
                    print('//////////////////////////////')
                    print('create a selection from cross')
                    for tab in left_cross.inputs:
                        cross_relations.append(str(tab))
                    for cond in reversed(global_conditions):
                        if type(cond.inputs[0]) == type(AttrRef) and type(cond.inputs[1]) == type(AttrRef):
                            if cond.inputs[0].rel in cross_relations and cond.inputs[1].rel in cross_relations:
                                left_cross = radb.ast.Select(cond, left_cross)
                                global_conditions.remove(cond)

    result = left_cross


    if len(global_conditions) > 0:
        for cond in reversed(global_conditions):
            result = radb.ast.Select(cond , result)


    global_inputs.clear()
    print('\n # cleare global inputs', global_inputs)
    global_conditions.clear()
    return result



def rule_merge_selections(stmt):
    print('\n')
    print('------- rule merge selections -----------')
    if type(stmt) == type(Select):
        print('merge select')
        while type(stmt) == type(Select):
            cond = stmt.cond
            stmt = stmt.inputs[0]
            global_conditions.append(cond)
            print('GC', global_conditions, stmt)
            if type(stmt) == type(Cross) and type(stmt.inputs[0]) == type(Select) and type(stmt.inputs[0].inputs[0]) == type(RelRef):
                for i, input in enumerate(stmt.inputs):
                    if type(input) == type(Select):
                        cond = input.cond
                        inputs = input.inputs
                        global_conditions.append(cond)
                        #print(global_conditions, inputs[0])
                        for j, cond in enumerate(global_conditions):
                            print(cond, stmt)
                            if j == 0:
                                conditions = cond
                            else:
                                conditions = radb.ast.ValExprBinaryOp(conditions, radb.ast.sym.AND, cond)

                        select = radb.ast.Select(conditions, inputs[0])
                    else:
                        select = inputs

                    if i == 0:
                        left_cross = select
                    else:
                        right_cross = rule_merge_selections(input)
                        left_cross = radb.ast.Cross(left_cross, right_cross)
                global_conditions.clear()
                return left_cross
            else:
                print('todo', type(stmt))



        for i, cond in enumerate(global_conditions):
            print(cond, stmt)
            if i == 0:
                conditions = cond
            else:
                conditions = radb.ast.ValExprBinaryOp(conditions, radb.ast.sym.AND, cond)
        global_conditions.clear()
        return radb.ast.Select(conditions, stmt)
        print('-----')

    elif type(stmt) == type(Cross):
        print('cross')
        inputs = stmt.inputs
        print(inputs)
        for i, input in enumerate(inputs):
            if i == 0:
                left_cross = rule_merge_selections(input)
            else:
                right_cross = rule_merge_selections(input)
                left_cross = radb.ast.Cross(left_cross, right_cross)
        return left_cross



    elif type(stmt) == type(Project):
        print('project >', stmt)
        attrs = stmt.attrs
        inputs = stmt.inputs
        select = rule_merge_selections(inputs[0])
        return radb.ast.Project(attrs, select)

    elif type(stmt) == type(RelRef):
        return stmt

    elif type(stmt) == type(Rename):
        return stmt
    else:
        print('not implemeted yes')

def check_cond(cond):
    if cond.op == 11:
        for cond in cond.inputs:
            if cond.op == 11:
                check_cond(cond)
            elif (type(cond.inputs[0]) == type(AttrRef) and type(cond.inputs[1]) == type(AttrRef)):
                return True
            else:
                return False
    else:
        if (type(cond.inputs[0]) == type(AttrRef) and type(cond.inputs[1]) == type(AttrRef)):
            return True
        else:
            return False



def rule_introduce_joins(stmt):
    print('\x1b[6;30;42m' + 'Rule Intro joins!' + '\x1b[0m')
    if type(stmt) == type(Select):
        print('\n')
        print('select')
        cond = stmt.cond
        print('>>>', cond)
        if check_cond(cond):
            print('checked', len(cond.inputs), type(cond.inputs), cond.op )
            if cond.op == 11:
                for c in cond.inputs:
                    print('>>', c, type(c), c.op)
                    if c.op == 11:
                        for c in c.inputs:
                            print(c, type(c))
                            global_conditions.append(c)
                    else:
                        print(c, type(c))
                        global_conditions.append(c)
            else:
                global_conditions.append(cond)

        else:
            select_conditions.append(cond)
        stmt = stmt.inputs[0]
        print('newstmt', stmt)
        print('glo cond', len(global_conditions))
        print('\n')
        return rule_introduce_joins(stmt)

    elif type(stmt) == type(Cross):
        print('# cross')
        inputs = stmt.inputs
        print(inputs[0],' - ', inputs[1])
        for i, input in enumerate(inputs):
            print(i, input)
            if i == 0:
                if type(input) != type(Cross) and type(input) != type(Select):
                    left_join = input
                    print('- left_join', input)
                else:
                    left_join = rule_introduce_joins(input)
            else:
                if type(input) != type(Cross) and type(input) != type(Select):
                    right_join = input
                    print('- right_join', right_join)

                else:
                    right_join = rule_introduce_joins(input)
                print('++ global conditions', global_conditions)

                    #if c.inputs[0].rel == left_join
                if len(global_conditions) > 0:
                    if global_conditions[0].inputs[0].rel == str(left_join) and global_conditions[0].inputs[1].rel == str(right_join):
                        last_index = 0
                    elif type(right_join) == type(Select) or type(left_join) == type(Select):
                        last_index = 0
                    else:
                        last_index = len(global_conditions) - 1
                    #
                    left_join = radb.ast.Join(left_join, global_conditions[last_index], right_join)
                    global_conditions.remove(global_conditions[last_index])
                    print('>> left_join', left_join)
                else:
                    print('final croos', global_conditions)
                    left_join = radb.ast.Cross(left_join, right_join)

        return left_join



    elif type(stmt) == type(Project):
        print('project >', stmt)
        attrs = stmt.attrs
        inputs = stmt.inputs
        print('_', inputs[0])
        select = rule_introduce_joins(inputs[0])
        return radb.ast.Project(attrs, select)

    elif type(stmt) == type(RelRef):
        if len(select_conditions) > 0:
            last_index = len(select_conditions) - 1
            select = radb.ast.Select(select_conditions[last_index], stmt)
            select_conditions.remove(select_conditions[last_index])
        else:
            select = stmt
        return select

    elif type(stmt) == type(Rename):
        print('>>', stmt, len(select_conditions))
        if len(select_conditions) > 0:
            last_index = len(select_conditions) - 1
            select = radb.ast.Select(select_conditions[last_index], stmt)
            select_conditions.remove(select_conditions[last_index])
        else:
            select = stmt
        return select
    else:
        print('not implemeted yet')

def break_cross(stmt):
    print('>> break_cross', type(stmt), stmt, len(stmt.inputs))
    if len(stmt.inputs) == 0:
        global_inputs.append((stmt))
    else:
        for input in stmt.inputs:
            print('in break cross', input)
            if type(input) == type(RelRef):
                global_inputs.append(input)
            elif type(input) == type(Rename):
                global_inputs.append(input)
            else:
                break_cross(input)


    return global_inputs

def break_condition(condition):
    for cond in condition.inputs:
        if cond.op == 11:
            break_condition(cond)
        else:
            global_conditions.append(cond)

def set_selection(conditions, inputs):
    print(conditions, inputs[0])
    select = rule_break_up_selections(inputs[0])
    print('** select in set_select', select, type(conditions))
    print(conditions.op)
    if conditions.op == 43:
        print(conditions)
        select = radb.ast.Select(conditions, select)
    else:
        for cond in reversed(conditions.inputs):
            print(len(cond.inputs))
            if len(cond.inputs) > 0 and cond.op == 11:
                print('conditions', cond.inputs)
                for cond in reversed(cond.inputs):
                    print(cond)
                    select = radb.ast.Select(cond, select)
            else:
                print('one condition', cond)
                select = radb.ast.Select(cond, select)

    return select

def set_project(attrs, input):
    return radb.ast.Project(attrs, input)

def set_cross(left, right):
    return radb.ast.Cross(left, right)


