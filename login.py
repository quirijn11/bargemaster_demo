from datetime import timedelta

import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import bcrypt
from pathlib import Path
import json
from streamlit_extras.switch_page_button import switch_page
from streamlit.source_util import _on_pages_changed, get_pages
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(layout="wide")

DEFAULT_PAGE = "login.py"
SECOND_PAGE_NAME = "Home"


# all pages request
def get_all_pages():
    default_pages = get_pages(DEFAULT_PAGE)

    pages_path = Path("pages.json")

    if pages_path.exists():
        saved_default_pages = json.loads(pages_path.read_text())
    else:
        saved_default_pages = default_pages.copy()
        pages_path.write_text(json.dumps(default_pages, indent=4))

    return saved_default_pages


# clear all page but not login page
def clear_all_but_first_page():
    current_pages = get_pages(DEFAULT_PAGE)

    if len(current_pages.keys()) == 1:
        return

    get_all_pages()

    # Remove all but the first page
    key, val = list(current_pages.items())[0]
    current_pages.clear()
    current_pages[key] = val

    _on_pages_changed.send()


# show all pages
def show_all_pages():
    current_pages = get_pages(DEFAULT_PAGE)

    saved_pages = get_all_pages()

    # Replace all the missing pages
    for key in saved_pages:
        if key not in current_pages:
            current_pages[key] = saved_pages[key]

    _on_pages_changed.send()


# Hide default page
def hide_page(name: str):
    current_pages = get_pages(DEFAULT_PAGE)

    for key, val in current_pages.items():
        if val["page_name"] == name:
            del current_pages[key]
            _on_pages_changed.send()
            break


# calling only default(login) page
clear_all_but_first_page()

st.markdown("""
    <style>
    .footer {
        position: fixed;
        left: 0;
        bottom: 0;
        width: 100%;
        background-color: white;
        color: black;
        text-align: center;
        padding: 10px;
    }
    .footer img {
        height: 50px;
    }
    </style>
    <div class="footer">
        <p>Powered by <img src="https://nederlandvacature.nl/werkgever/logo/37021/" alt="Logo"></p>
    </div>
    """, unsafe_allow_html=True)


# Login form
def login():
    # Clearing Streamlit session state (if applicable)
    if 'authentication_status' in st.session_state:
        del st.session_state['authentication_status']
    st.header("Login")
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)

    authenticator = stauth.Authenticate(
        config['credentials']
    )
    if authenticator.login() is not None:
        name, authentication_status, username = authenticator.login()

    if st.session_state["authentication_status"]:
        authenticator.logout('Logout', 'main')
        # Redirect to the desired page
        show_all_pages()  # call all page
        hide_page(DEFAULT_PAGE.replace(".py", ""))  # hide first page
        switch_page(SECOND_PAGE_NAME)  # switch to second page
        st.session_state['generate_dashboard'] = True
        st.session_state['files'] = True

        if st.session_state.generate_dashboard and st.session_state.files:
            check = True
        elif not st.session_state["authentication_status"]:
            st.error('Username/password is incorrect')
        elif st.session_state["authentication_status"] is None:
            st.warning('Please enter your username and password')


# Run the Streamlit app
def main():
    # Display the login or sign-up form based on user selection
    form_choice = st.selectbox("Select an option:", ("Login"))

    if form_choice == "Login":
        login()


if __name__ == '__main__':
    main()

# Footer with logo
