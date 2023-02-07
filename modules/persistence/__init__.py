# import sqlalchemy as alchemy
# from sqlalchemy.orm import Session, DeclarativeBase, Mapped, mapped_column, relationship
# from sqlalchemy import text, MetaData, Table, Column, Integer, String, ForeignKey, insert, select, bindparam, \
#     literal_column, and_, or_, func
# from typing import List, Optional
#
# print(f'\n#################################################')
# print(f'#\t\t\tSQLAlchemy version: {alchemy.__version__}\t\t\t#')
# print(f'#################################################')
#
# engine = alchemy.create_engine("sqlite+pysqlite:///:memory:", echo=True)
#
# """
# Always use connection object inside a context manager (python with statement)
# by default a rollback is emitted at the end of the block
# """
# with engine.connect() as conn:
#     result = conn.execute(text("select 'hello world'"))
#     print(result.all())
#
# """
# use commit to commit the queries
# use results within the context
# \"commit as you go\"
# """
# with engine.connect() as conn:
#     conn.execute(text('Create table test (x int, y int)'))
#     conn.execute(text('insert into test (x, y) values(:x, :y)'), [{'x': 1, 'y': 1}, {'x': 2, 'y': 4}])
#     conn.commit()
#
# """
# transaction block up front
# encloses everything inside the transaction with a COMMIT at the end
# Begin once
# """
# with engine.begin() as conn:
#     conn.execute(text('insert into test (x, y) values(:x, :y)'), [{'x': 3, 'y': 6}, {'x': 4, 'y': 8}])
#
# with engine.connect() as conn:
#     result = conn.execute(text('select * from test'))
#     for x, y in result:
#         print(f'x: {x}, y: {y}')
#
# with engine.connect() as conn:
#     result = conn.execute(text('select x, y from test where y > :y'), {'y': 5})
#     for i in result.mappings():
#         print(f'x: {i["x"]}, y: {i["y"]}')
#
# """
# using ORM from sqlalchemy
# """
#
# with Session(engine) as session:
#     stm = text("select x, y from test where y> :y order by x, y desc")
#     result = session.execute(stm, {'y': 5})
#     for x, y in result:
#         print(f'x: {x}, y: {y}')
#
# with Session(engine) as session:
#     stm = text("update test set y=:y where x = :x")
#     result = session.execute(stm, [{'x': 3, 'y': 16}, {'x': 1, 'y': 101}])
#     session.commit()
#
# """
# Working with database meta data
# """
#
# meta_obj = MetaData()
#
# user_table = Table('user_table', meta_obj,
#                    Column(name='id', type_=Integer, primary_key=True),
#                    Column(name='name', type_=String(30)),
#                    Column(name='fullname', type_=String)
#                    )
#
# print(user_table.primary_key)
#
# address_table = Table('address', meta_obj,
#                       Column(name='id', type_=Integer, primary_key=True),
#                       Column('user_id', ForeignKey('user_table.id'), nullable=False),
#                       Column(name='email_address', type_=String, nullable=False)
#                       )
#
# """
# Emitting DDL to the database
# """
# meta_obj.create_all(engine)
#
# """
# ORM Declarative forms
# """
#
#
# class Base(DeclarativeBase):
#     pass
#
#
# print(Base.metadata)
# print(Base.registry)
#
#
# class User(Base):
#     __tablename__ = 'user_table'
#     id: Mapped[int] = mapped_column(primary_key=True)
#     name: Mapped[str] = mapped_column(String(30))
#     fullname: Mapped[Optional[str]]
#     addresses: Mapped[List['Address']] = relationship(back_populates='user')
#
#     def __repr__(self) -> str:
#         return f'User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})'
#
#
# class Address(Base):
#     __tablename__ = 'address'
#     id: Mapped[int] = mapped_column(primary_key=True)
#     email_address: Mapped[str]
#     user_id = mapped_column(ForeignKey('user_table.id'))
#
#     user: Mapped[User] = relationship(back_populates='addresses')
#
#     def __repr__(self) -> str:
#         return f'Address(id={self.id!r}, email_address={self.email_address!r})'
#
#
# andy = User(name='andy', fullname='Andy OB')
# meta_obj2 = Base.metadata
# meta_obj2.create_all(engine)
#
# test = Table('test', meta_obj2, autoload_with=engine)
#
# """
# Working with Data
# """
#
# stm = insert(user_table).values(name='spongebob', fullname='Spongebob Squarepants')
# print(stm, type(stm))
# compiled = stm.compile()
# print(compiled, type(compiled))
# print(compiled.params)
#
# with engine.connect() as conn:
#     result = conn.execute(stm)
#     conn.commit()
#     print(result.inserted_primary_key)
#     conn.execute(insert(user_table),
#                  [{'name': 'sandy', 'fullname': 'Sandy Koskhol'}, {'name': 'dariush', 'fullname': 'Dariush Amali'}])
#     conn.commit()
#
# subq = (select(user_table.c.id).where(user_table.c.name == bindparam('username')).scalar_subquery())
#
# with engine.connect() as conn:
#     conn.execute(insert(address_table).values(user_id=subq),
#                  [{'username': 'sandy', 'email_address': 'sandy@sqlalchemy.org'},
#                   {'username': 'sandy', 'email_address': 'sanyKoskhol@sqlalchemy.org'},
#                   {'username': 'dariush', 'email_address': 'dariush@amali.org'}])
#     conn.commit()
#     result = conn.execute(select(address_table))
#     print(result.all())
#
# select_from = select(user_table.c.id, user_table.c.name + '@parstasmim.com')
# with engine.connect() as conn:
#     insert_stm = insert(address_table).from_select(['user_id', 'email_address'], select_from)
#     # .returning(address_table.c.id, address_table.c.user_id, address_table.c.email_address)
#     # insert_stm = insert(user_table)
#     result = conn.execute(insert_stm)
#     conn.commit()
#     print(result)
#
# """
# SELECT
# """
# select_stm = select(user_table).where(user_table.c.name == 'dariush')
# print(select_stm)
# with engine.connect() as conn:
#     result = conn.execute(select_stm)
#     for row in result:
#         print(row)
#
# """
# ORM SELECT
# """
#
# select_stm = select(User).where(User.name == 'sandy')
#
# with Session(engine) as session:
#     result = session.execute(select_stm)
#     for row in result:
#         print(row)
#
# select_stm = select((text('"You are fucking "') + user_table.c.name).label('fuck')).order_by(user_table.c.id)
#
# with engine.connect() as conn:
#     result = conn.execute(select_stm)
#     for row in result:
#         print(f'{row.fuck}')
#
# select_stm = select(literal_column('"Hey"').label('a'), user_table.c.name.label('b')).order_by('b')
#
# with engine.connect() as conn:
#     result = conn.execute(select_stm)
#     for r in result:
#         print(f'{r.a} {r.b}')
#
# """
# WHERE
# """
#
# # select_stm = select(user_table.c.name,address_table.c.email_address).where(user_table.c.id == address_table.c.user_id, user_table.c.id > 1)
# # select_stm = select(user_table.c.name,address_table.c.email_address).where(and_(user_table.c.id == address_table.c.user_id, or_(user_table.c.id > 2,user_table.c.id ==1)))
# select_stm = select(User.name, Address.email_address).filter_by(name = 'sandy').where(User.id == Address.user_id)
#
# with engine.connect() as conn:
#     print(select_stm)
#     for row in conn.execute(select_stm):
#         print(row)
#
# """
# AND between where clauses
# """
#
# select_stm = select(User.name, Address.user_id, Address.email_address).where(User.id == Address.user_id, User.name == 'sandy', Address.id > 2)
# select_and_or = select(User.name, Address.user_id, Address.email_address).where(and_(or_(User.name == 'sandy', User.name == 'dariush'), and_(User.id == Address.user_id, Address.email_address.contains('pars'))))
#
# with Session(engine) as session:
#     result = session.execute(select_stm)
#     result1 = session.execute(select_and_or)
#     for row in result:
#         print(f'row: {row}')
#     for row in result1:
#         print(f'Second Select row: {row}')
#
# """
# Join Clause
# """
#
# # select_stm = select(user_table.c.name, address_table.c.email_address).join_from(user_table, address_table)
# # select_stm = select(user_table.c.name, address_table.c.email_address).join(address_table)
# select_stm = select(address_table.c.id, address_table.c.email_address, user_table.c.fullname).select_from(user_table).join(address_table)
#
# with engine.connect() as conn:
#     result = conn.execute(select_stm)
#     for i in result:
#         print(f'result: {i}')
#
# print(select(func.count("*")).select_from(user_table))
#
