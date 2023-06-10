from src.pipeline.batch_prediction import BatchPrediction
from src.pipeline.batch_prediction import BatchPredictionConfig


if __name__=="__main__":
    config = BatchPredictionConfig()
    gemstone_batch_prediction = BatchPrediction(batch_config=config)
    gemstone_batch_prediction.start_prediction() 