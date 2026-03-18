import polars as pl

fable = pl.read_parquet("data/fable.parquet")
gr = pl.read_parquet("data/2023_2024-gr.parquet")

# Harmonize fable
fable_clean = fable.select(
    pl.lit("fable").alias("source"),
    pl.col("title"),
    pl.col("author_1").alias("author"),
    pl.col("author_2").alias("additional_authors"),
    pl.col("rating"),
    pl.col("review"),
    pl.col("created_at").alias("date_read"),
    pl.col("page_count"),
    pl.col("published_date"),
    pl.col("review_average").alias("community_avg_rating"),
    pl.col("review_count"),
    pl.col("rating_difference"),
    pl.col("subjects"),
    pl.col("genre_1"),
    pl.col("genre_2"),
    pl.col("genre_3"),
    pl.col("genre_4"),
    pl.col("genre_5"),
    pl.col("genre_6"),
    pl.col("cover_image"),
    pl.col("background_color"),
    pl.col("price_usd"),
    # GR-only cols — null for fable rows
    pl.lit(None).cast(pl.String).alias("isbn"),
    pl.lit(None).cast(pl.String).alias("isbn13"),
    pl.lit(None).cast(pl.Int64).alias("book_id"),
    pl.lit(None).cast(pl.String).alias("publisher"),
    pl.lit(None).cast(pl.String).alias("binding"),
    pl.lit(None).cast(pl.Int64).alias("year_published"),
    pl.lit(None).cast(pl.Int64).alias("original_publication_year"),
    pl.lit(None).cast(pl.String).alias("date_added"),
    pl.lit(None).cast(pl.String).alias("bookshelves"),
    pl.lit(None).cast(pl.String).alias("exclusive_shelf"),
)

# Harmonize GR — rating 0 means "no rating" in GR exports
gr_clean = gr.select(
    pl.lit("goodreads").alias("source"),
    pl.col("Title").alias("title"),
    pl.col("Author").alias("author"),
    pl.col("Additional Authors").alias("additional_authors"),
    pl.when(pl.col("My Rating") == 0)
      .then(None)
      .otherwise(pl.col("My Rating").cast(pl.Float64))
      .alias("rating"),
    pl.col("My Review").alias("review"),
    pl.col("Date Read")
      .str.to_date("%Y/%m/%d", strict=False)
      .cast(pl.Datetime)
      .alias("date_read"),
    pl.col("Number of Pages").alias("page_count"),
    pl.col("Year Published").cast(pl.String).alias("published_date"),
    pl.col("Average Rating").alias("community_avg_rating"),
    # Fable-only cols — null for GR rows
    pl.lit(None).cast(pl.Int64).alias("review_count"),
    pl.lit(None).cast(pl.Float64).alias("rating_difference"),
    pl.lit(None).cast(pl.List(pl.String)).alias("subjects"),
    pl.lit(None).cast(pl.String).alias("genre_1"),
    pl.lit(None).cast(pl.String).alias("genre_2"),
    pl.lit(None).cast(pl.String).alias("genre_3"),
    pl.lit(None).cast(pl.String).alias("genre_4"),
    pl.lit(None).cast(pl.String).alias("genre_5"),
    pl.lit(None).cast(pl.String).alias("genre_6"),
    pl.lit(None).cast(pl.String).alias("cover_image"),
    pl.lit(None).cast(pl.String).alias("background_color"),
    pl.lit(None).cast(pl.String).alias("price_usd"),
    # GR-only cols
    pl.col("ISBN").alias("isbn"),
    pl.col("ISBN13").alias("isbn13"),
    pl.col("Book Id").alias("book_id"),
    pl.col("Publisher").alias("publisher"),
    pl.col("Binding").alias("binding"),
    pl.col("Year Published").alias("year_published"),
    pl.col("Original Publication Year").alias("original_publication_year"),
    pl.col("Date Added").alias("date_added"),
    pl.col("Bookshelves").alias("bookshelves"),
    pl.col("Exclusive Shelf").alias("exclusive_shelf"),
)

combined = pl.concat([fable_clean, gr_clean]).sort("date_read", nulls_last=True)

combined.write_parquet("data/combined.parquet")
