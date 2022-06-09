def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[1]).toDF()
    df = DynamicFrame.fromDF(df, glueContext, "test")
    return DynamicFrameCollection({"CustomTransform0": df}, glueContext)