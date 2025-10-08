from fastapi import APIRouter

router = APIRouter()

@router.get("/bytesCalculator")
async def calculate_bytes():
    return {"message": "Byte calculation successful!"}