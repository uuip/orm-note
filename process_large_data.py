def upgrade_data1():
    sql = """
             set global group_replication_transaction_size_limit=600000000;  
            """
    op.execute(sql)
    OLD = 'langgenius/openai_api_compatible'
    NEW = 'langgenius/openai_api_compatible4'
    BATCH_SIZE = 500
    with op.get_context().autocommit_block():
        SessionMaker = sessionmaker(bind=op.get_bind())
        workflows_to_update = []
        stmt = select(Workflow).execution_options(yield_per=BATCH_SIZE)
        with SessionMaker() as session:
            for wf in session.scalars(stmt):
                if OLD in wf.graph:
                    workflows_to_update.append({"id": wf.id, "graph": wf.graph.replace(OLD, NEW)})
        with SessionMaker() as session:
            for i in range(0, len(workflows_to_update), BATCH_SIZE):
                batch = workflows_to_update[i : i + BATCH_SIZE]
                session.execute(update(Workflow), batch)
                session.commit()
    print(f"总共更新了 {len(workflows_to_update)} 条Workflow记录")