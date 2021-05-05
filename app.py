import streamlit as st
import numpy as np
import pandas as pd
import datetime
import os


def run_app():

    def call_api(from_date, to_date):
        """
        A function load data
        :params from_date and to_date: to request json data from the flask API
        :return: json as pandas.DataFrame instance
        """
        root = f"{os.getcwd()}"
        df = pd.read_csv(f'{root}/data/test_data.csv')
        df.set_index('Datetime', inplace=True)
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        return df

    st.title("Some basic charts!!")

    # to_date = datetime.date.today() - datetime.timedelta(days=1)
    # from_date = to_date- datetime.timedelta(days=90)
    # from_date = st.date_input('Start date', from_date)
    # to_date = st.date_input('End date', to_date)  

    # recent_data = call_api(from_date, to_date)
    # st.write(recent_data)

    # area = st.multiselect('Choose the location(s) to analyze', ['']+recent_data.columns.to_list())
    # st.write(recent_data['comments'])





def main():
    st.sidebar.markdown("""
        <h2>Choose the mode:</h2>    
    """, unsafe_allow_html=True)
    mode = st.sidebar.selectbox('', [
        'Dashboard!!',
        'About the Project!',
    ])
    if mode == 'About Us!':
        st.title('Details of the project!')

        st.markdown("""
                need to write as many details as possible
            """)


    elif mode == 'Analytics Dashboard':
        st.title('Wall Street Bets Daily Discussion Thread Analysis')

        st.markdown("""
            Got to create some charts here
            """)
        # run_app()


if __name__ == '__main__':
    main()        