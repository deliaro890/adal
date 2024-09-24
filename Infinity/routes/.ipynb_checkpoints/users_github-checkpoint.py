from fastapi import APIRouter
from pydantic import BaseModel
from requests import get
#from middlewares.verify_token_route import  VerifyTokenRoute
#

users_github = APIRouter() #Esta es la que se importa desde el main

class UserGithub (BaseModel):
    country : str
    page : str 


@users_github.post("/github/users")
def github_users (github : UserGithub):
    #print ( get(f'https://api.github.com/search/users?q=location:"{github.country}"&page={github.page}').json() )
    return get(f'https://api.github.com/search/users?q=location:"{github.country}"&page={github.page}').json()

