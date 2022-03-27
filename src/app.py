from fastapi import FastAPI, responses

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    print('startup')


@app.get("/index")
async def root():
    return responses.HTMLResponse("<h>app</h>")
