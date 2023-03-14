import requests
import os
import dotenv

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)


def scrape_indeed():
    # ONLY RUN THIS ON FIRST TIME API SET UP
    # code = request_auth_code()
    # request_user_access_token(code)

    # ONLY CALL REFRESH TOKEN WHEN API CALLS FAIL
    # DUE TO EXPIRATION OF ACCESS TOKEN
    # request_refresh_token()

    response = requests.get(
        "https://sg.indeed.com/companies/search?q=apple"
    )

    print(response.text)


def request_auth_code():
    # tried to make commented code dynamic so no need use user input
    # but Indeed too dog alr

    # https://secure.indeed.com/oauth/v2/authorize?client_id=0728674de11cf07bcb32c61eb19466e123d46df843c6a81bd0fd396cf244ccf7&redirect_uri=http%3A%2F%2Flocalhost&response_type=code&state=ict2107&scope=email+offline_access+employer_access
    # url = f"https://secure.indeed.com/oauth/v2/authorize?" \
    #       f"client_id={INDEED_API['client_id']}&" \
    #       f"redirect_uri=http%3A%2F%2F{INDEED_API['redirect_uri']}&" \
    #       f"response_type=code&" \
    #       f"state=ict2107&" \
    #       f"scope=email+offline_access+employer_access"
    # result = requests.get(
    #     url
    # )
    # print(result)

    print("CLICK ON THE LINK BELOW:")
    print(f"https://secure.indeed.com/oauth/v2/authorize?"
          f"client_id={os.environ['CLIENT_ID']}&"
          f"redirect_uri=http%3A%2F%2Flocalhost&"
          f"response_type=code&"
          f"state=ict2107&"
          f"scope=email+offline_access+employer_access")

    print("\nLOOK FOR CODE IN URL eg. localhost/......code=gHKRerZCuZM")
    code = input("Copy paste and enter auth code:\n")
    print("CODE WILL EXPIRE IN 10 MINUTES")
    return code


def request_user_access_token(code):
    response = requests.post(
        "https://apis.indeed.com/oauth/v2/tokens",
        headers={
            'Content-Type': "application/x-www-form-urlencoded",
            'Accept': "application/json"
        },
        data={
            'code': code,
            'client_id': os.environ['CLIENT_ID'],
            'client_secret': os.environ['CLIENT_SECRET'],
            'redirect_uri': os.environ['REDIRECT_URI'],
            'grant_type': "authorization_code"
        }
    ).json()

    for key, value in response.items():
        if key == "access_token":
            os.environ['ACCESS_TOKEN'] = value
            dotenv.set_key(dotenv_file, "ACCESS_TOKEN", os.environ["ACCESS_TOKEN"])
        if key == "id_token":
            os.environ['ID_TOKEN'] = value
            dotenv.set_key(dotenv_file, "ID_TOKEN", os.environ["ID_TOKEN"])
        if key == "refresh_token":
            os.environ['REFRESH_TOKEN'] = value
            dotenv.set_key(dotenv_file, "REFRESH_TOKEN", os.environ["REFRESH_TOKEN"])

    print("IMPORTANT TOKENS HAVE BEEN INSERTED INTO SECRET FILE")


def request_refresh_token():
    response = requests.post(
        "https://apis.indeed.com/oauth/v2/tokens",
        headers={
            'Content-Type': "application/x-www-form-urlencoded",
            'Accept': "application/json"
        },
        data={
            'refresh_token': os.environ['REFRESH_TOKEN'],
            'client_id': os.environ['CLIENT_ID'],
            'client_secret': os.environ['CLIENT_SECRET'],
            'grant_type': "refresh_token"
        }
    ).json()

    for key, value in response.items():
        if key == "refresh_token":
            os.environ['REFRESH_TOKEN'] = value
            dotenv.set_key(dotenv_file, "REFRESH_TOKEN", os.environ["REFRESH_TOKEN"])