import psycopg2

class DataBase:
    def __init__(self) -> None:
        self.database = psycopg2.connect(
            database = '11-dars',
            user = 'postgres',
            host = 'localhost',
            password = '1'
        )
        
    def manager(self,sql,*args,commit=False,fetchone=False,fetchall=False):
        with self.database as db:
            with db.cursor() as cursor:
                cursor.execute(sql,args)
                if commit:
                    db.commit()
                elif fetchone:
                    return cursor.fetchone()
                elif fetchall:
                    return cursor.fetchall()
                
    def drop_tables(self):
        sqls = [
            'drop table if exists categories cascade;',
            'drop table if exists news cascade;',
            'drop table if exists comments cascade;'
        ]
        
        for sql in sqls:
            db.drop_tables(*sql)
                
    def create_tables(self):
        sqls = [
            '''
            create table if not exists categories(
                id serial primary key,
                category_name varchar(100) not null,
                description text
                );''',
                
            '''
            create table if not exists news(
                id serial primary key,
                category_id integer references categories(id),
                title varchar(200) not null,
                content text not null,
                published_at timestamp default current_timestamp,
                is_published bool default false
            );''',
            
            '''
            create table if not exists comments(
                id serial primary key,
                news_id integer references news(id),
                author_name varchar(100),
                comment_text text not null,
                created_at timestamp default current_timestamp
            );'''
            ]
        
        for sql in sqls:
            db.manager(sql,commit=True)
            
    def alter_table_column_add(self):
        sql = '''alter table news add column views  integer default 0 '''
        self.manager(sql,commit=True)
        
    def insert_categories(self,category_name,description):
        sql = '''insert into categories(category_name,description) values (%s,%s)'''
        self.manager(sql,category_name,description,commit=True) 
        
    def insert_news(self,category_id,title,content,is_published):
        sql = '''insert into news(category_id,title,content,is_published) values (%s,%s,%s,%s)'''
        return self.manager(sql,category_id,title,content,is_published,commit=True)
    
    def insert_into_comments(self,news_id,author_name,comment_text):
        sql = '''insert into comments(news_id,author_name,comment_text) values (%s,%s,%s)'''
        self.manager(sql,news_id,author_name,comment_text,commit=True)
        
    def select_categories(self):
        sql = 'select * from categories;'
        return self.manager(sql,fetchall=True)
            
    def select_news(self):
        sql = 'select category_id,title from news;'
        return self.manager(sql,fetchall=True)
            
    def select_comments(self):
        sql = 'select * from comments;'
        return self.manager(sql,fetchall=True)
  
    def update(self):
        sql = '''update news set views = views + 1 '''
        self.manager(sql,commit=True)
        
    def is_published(self):
        sql = '''update news set is_published = True where published_at < now() -interval '1 day' and is_published = false '''
        self.manager(sql,commit=True)
        
    def delete(self):
        sql = '''delete from comments where created_at > now() -interval '1 year';'''
        self.manager(sql,commit=True)
        
    def select_alias_news(self):
        sql = '''select n.id as news_id, n.title as news_title, c.category_name as category_name
        from news as n
        join categories as c on n.category_id = c.id;
        '''
        return self.manager(sql,fetchall=True)
    
    def select_Technology(self):
        sql = '''select category_name from categories where category_name = 'Technology'; '''
        return self.manager(sql,fetchall=True)
        
    def select_elon_news(self):
        sql = '''select id,title,content,published_at from news
        where is_published = True
        order by published_at desc
        limit 5;'''
        return self.manager(sql,fetchall=True)
    
    def select_views(self):
        sql = '''select * from news where views between 10 and 100;'''
        return self.manager(sql,fetchall=True)

    def author_name_like(self,value):
        sql = '''select * from comments where author_name like %s;'''
        return self.manager(sql,value,fetchall=True)
    
    def author_name_bosh(self):
        sql = '''select * from comments where author_name is null;'''
        return self.manager(sql,fetchall=True)
        
    def news_count(self):
        sql = '''select c.category_name, count(n.id) as news_count
        from categories as c
        left join news as n on c.id = n.category_id
        group by c.category_name;
        '''    
        return self.manager(sql,fetchall=True)
    
    def add_unique_title(self):
        sql = '''alter table news add constraint unique_title unique (title);'''
        return self.manager(sql,fetchall=True)
    
db = DataBase()
db.create_tables()
# db.alter_table_column_add()
# db.update()
# db.is_published()
# db.delete()
# db.drop_tables()

# ===============================
categories = [
    ('Technology','hgfd'),
    ('Sports','rtdffh'),
    ('Health','ertgds')
]
for categorie in categories:
    db.insert_categories(*categorie) 
    
news = [
    (1,'haqiqat','ohho',True),
    (2,'jazo','yaxshi',True),
    (3,'olim','yomon',True),
]
for new in news:
    db.insert_news(*new)
                
comments = [
    (1,None,'allambalo'),
    (2,'hasanboy','qorqinchili'),
    (3,'Abdulbosit','yomon')
]
for comment in comments:
    db.insert_into_comments(*comment)
# =======================
# print(db.select_categories())
# print(db.select_news())
# print(db.select_comments())
# print(db.select_alias_news())
# print(db.select_Technology())
# print(db.select_elon_news())
# print(db.select_views())
# print(db.author_name_like('A%'))
# print(db.author_name_bosh())
# print(db.news_count())
print(db.add_unique_title())