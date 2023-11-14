import streamlit as st
import pandas as pd
import openai
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
from langchain.chat_models import ChatOpenAI
from langchain.agents.agent_types import AgentType
from langchain.llms import OpenAI


def chatgpt_connect(df):
    agent = create_pandas_dataframe_agent(
        ChatOpenAI(temperature=0, model="gpt-3.5-turbo-0613"),
        df,
        verbose=True,
        agent_type=AgentType.OPENAI_FUNCTIONS,
    )

    prompt = st.text_area("Enter your question:")
    df_columns = list(df.columns)

    if st.button("Generate response"):
        if prompt:
            with st.spinner():
                # Pass the DataFrame columns as part of the prompt
                prompt_with_columns = f"{prompt} -df_columns {df_columns}"
                response = agent.run(prompt_with_columns)
                st.write(response)
        else:
            st.warning("Please enter a question.")
