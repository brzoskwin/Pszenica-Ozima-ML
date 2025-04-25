from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import joblib
from pathlib import Path

SCIEZKA_MODELI = Path("models")
model = joblib.load(SCIEZKA_MODELI / "rf_model_plonow.pkl")

app = FastAPI(title="API Predykcji Plonów Pszenicy")


class FertilizerInput(BaseModel):
    N: float
    P: float
    K: float
    temperatura: float
    opady: float
    klaster_finansowy: int
    klaster_pogodowy: int


@app.post("/predict")
def predict_yield(input_data: FertilizerInput):
    features = [
        'Nawożenie N [kg/ha]', 'Nawożenie P [kg/ha]', 'Nawożenie K [kg/ha]',
        'Średnia temperatura [°C]', 'Suma opadów [mm]',
        'Klaster_Finansowy', 'Klaster_Pogodowy'
    ]

    input_df = pd.DataFrame([[
        input_data.N, input_data.P, input_data.K,
        input_data.temperatura, input_data.opady,
        input_data.klaster_finansowy, input_data.klaster_pogodowy
    ]], columns=features)

    prediction = model.predict(input_df)[0]
    return {"przewidywany_plon": float(prediction)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
