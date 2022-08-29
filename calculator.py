from fastapi import FastAPI, Request, HTTPException
from json.decoder import JSONDecodeError
from converter import ConvertUnits, ConvertCurrencies, ConvertPrices

calculator = FastAPI()


@calculator.post("/api/v1/calculator/units")
async def convert_units(info: Request):
    try:
        req_info = await info.json()
    except JSONDecodeError:  # includes simplejson.decoder.JSONDecodeError
        raise HTTPException(status_code=400, detail="Input values must be integer or float")
    calorific_value = req_info['calorific_value']
    input_value = req_info['input_value']

    if not (isinstance(input_value, int) or isinstance(input_value, float)):
        raise HTTPException(status_code=400, detail="Input value must be integer or float")
    if not (isinstance(calorific_value, int) or isinstance(calorific_value, float)):
        raise HTTPException(status_code=400, detail="Calorific value must be integer or float")

    input_unit = req_info['input_unit']
    output_unit = req_info['output_unit']
    output_value = ConvertUnits().calculate(calorific_value=calorific_value,
                                            input_unit=input_unit,
                                            output_unit=output_unit,
                                            input_value=input_value)[0]
    return {
        "output_value": output_value,
        "data": req_info
    }


@calculator.post("/api/v1/calculator/currencies")
async def convert_currencies(info: Request):
    try:
        req_info = await info.json()
    except JSONDecodeError:  # includes simplejson.decoder.JSONDecodeError
        raise HTTPException(status_code=400, detail="Input values wrong format")
    date = req_info['date']
    input_value = req_info['input_value']
    input_currency = req_info['input_currency']
    output_currency = req_info['output_currency']
    result = ConvertCurrencies.calculate(input_value=input_value,
                                         input_currency=input_currency,
                                         output_currency=output_currency,
                                         date=date)
    output_value = result[0]
    date1 = result[1]
    date2 = result[2]
    return {
        "output_value": output_value,
        "date1": date1,
        "date2": date2,
        "data": req_info
    }


@calculator.post("/api/v1/calculator/prices")
async def convert_prices(info: Request):
    try:
        req_info = await info.json()
    except JSONDecodeError:  # includes simplejson.decoder.JSONDecodeError
        raise HTTPException(status_code=400, detail="Input values wrong format")

    input_value = req_info['input_value']
    calorific_value = req_info['calorific_value']
    input_unit = req_info['input_unit']
    output_unit = req_info['output_unit']
    input_currency = req_info['input_currency']
    output_currency = req_info['output_currency']
    date = req_info['date']
    output_value = ConvertPrices().calculate(input_value=input_value,
                                             calorific_value=calorific_value,
                                             input_unit=input_unit,
                                             output_unit=output_unit,
                                             input_currency=input_currency,
                                             output_currency=output_currency,
                                             date=date)
    source = "Investing"
    return {
        "output_value": output_value,
        "source": source,
        "data": req_info
    }
