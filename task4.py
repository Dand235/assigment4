import pandas
import yaml
import pyarrow.parquet as pq
import re
import networkx as nx
import streamlit as st

st.title('Task #4')
tabs= st.tabs(["DATA1", "DATA2", "DATA3"])
fold_count = 1
for tab in tabs:

    with tab:

    #for fold_count in range(1,4):
        book_file = f"data/DATA{fold_count}/books.yaml"
        with open(book_file, 'r') as f:

            books_data = yaml.safe_load(f)

        parquet=f"data/DATA{fold_count}/orders.parquet"
        df_orders=pandas.read_parquet(parquet)



        df_books=pandas.DataFrame(books_data)

        df_books.columns=df_books.columns.str.replace(':','')

        df_users=pandas.read_csv(f'data/DATA{fold_count}/users.csv')




        df_orders['timestamp'] = df_orders['timestamp'].str.replace(';', ' ').str.replace(',', ' ')

        df_orders['timestamp'] = df_orders['timestamp'].str.replace('A.M.', 'AM').str.replace('P.M.', 'PM')

        df_orders['datetime'] = pandas.to_datetime(df_orders['timestamp'], errors='coerce')

        df_orders['datetime'] =    df_orders['datetime'].dt.date



        df_orders['unit_price'] = df_orders['unit_price'].str.replace('¢', '.').str.replace('USD', '').str.replace('$', '')

        df_orders['unit_price']=df_orders['unit_price'].apply(
            lambda row: float(re.search(r"\d*\.\d+|\d+", str(row)).group(0)) * 1.2 if re.search(r'EUR|€', str(row)) else re.search(r"\d*\.\d+|\d+", str(row)).group(0)
        )
        df_orders['unit_price']=df_orders['unit_price'].astype(float)

        df_orders['paid_price']=df_orders['quantity']* df_orders['unit_price']


        map_address_user=dict(zip(df_users['id'], df_users['address']))


        df_orders['shipping'] = df_orders['shipping'].fillna(
            df_orders['user_id'].map(map_address_user)
        )

        df_orders.dropna(inplace=True)

        map_address_order=dict(zip(df_orders['user_id'], df_orders['shipping']))

        df_users['address'] = df_users['address'].fillna(
            df_orders['user_id'].map(map_address_order)
        )

        df_books.dropna(inplace=True)


        Gr=nx.Graph()

        Gr.add_nodes_from(df_users.id)

        groups = [
            ["address", "phone", "email"],
            ["name", "phone", "email"],
            ["name", "address", "email"],
            ["name", "address", "phone"],
        ]

        for cols in groups:

            dup = df_users[df_users.duplicated(cols, keep=False)]

            for _, group in dup.groupby(cols):

                ids = group["id"].tolist()

                for i in range(len(ids) - 1):

                    Gr.add_edge(ids[i], ids[i + 1])

        num_real_users = nx.number_connected_components(Gr)


        id_mapping = {}
        actual_id = 1

        for component in nx.connected_components(Gr):
            for user_id in component:
                id_mapping[user_id] = actual_id
            actual_id += 1

        df_users['actual_id'] = df_users['id'].map(id_mapping)



        authors=[]
        for author in df_books['author']:

            set_author=set(author.split(','))
            if(set_author not in authors):
                authors.append(set_author)

        df_books['authors_set'] = df_books['author'].str.split(',').apply(lambda x: set(x))





        quant=[]
        for author in authors:
            df_ids=df_books[df_books['authors_set'].isin([author])]['id'].tolist()
            quantity_sold=df_orders[df_orders['book_id'].isin(df_ids)].quantity.sum()
            quant.append(quantity_sold)


        author_quant=zip(authors, quant)
        sorted_authors=sorted(author_quant, key=lambda x: x[1],reverse=True)
        most_pop_author=sorted_authors[0][0]



        actual_id_money=[]
        for ido in df_users.actual_id:

            df_ids=df_users[df_users['actual_id'] == ido]['id'].tolist()

            money=df_orders[df_orders['user_id'].isin(df_ids)]['paid_price'].sum()

            actual_id_money.append((ido, money))

        actual_id_money=sorted(actual_id_money,key=lambda x: x[1],reverse=True)

        highest_spender=actual_id_money[0][0]



        dates_orders=df_orders['datetime'].unique()
        st.header('5 largest revenue days')

        revenue_daily=df_orders.groupby('datetime')['paid_price'].sum()

        st.write(revenue_daily.nlargest(5))


        col1, col2= st.columns(2)

        with col1:
            st.metric(label="Number of unique users:", value=num_real_users)
        with col2:
            st.metric(label="Number of unique sets of authors:", value=len(authors))

        col3, col4= st.columns(2)

        with col3:
            st.metric(label="Most popular author:", value=", ".join(map(str,most_pop_author)))
        with col4:
            st.metric(label="Best buyer id:", value=','.join(map(str,df_users[df_users['actual_id'] == highest_spender]['id'].tolist())))

        st.header('Daily revenue:')
        st.line_chart(revenue_daily)

        fold_count+=1

