from data_federation.parser.parser_pipeline import input_pipeline
from data_federation.input_model import SmallflexInputSchema


if __name__=="__main__":
    small_flex_input_schema: SmallflexInputSchema = input_pipeline() 