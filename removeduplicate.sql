
CREATE FUNCTION dbo.RemoveDuplicateWords
(
    @Input NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @Result NVARCHAR(MAX);

    ;WITH SplitWords AS (
        SELECT 
            LTRIM(RTRIM(value)) AS Word,
            ROW_NUMBER() OVER (PARTITION BY LTRIM(RTRIM(value)) ORDER BY (SELECT NULL)) AS rn
        FROM STRING_SPLIT(@Input, ' ')
    ),
    UniqueWords AS (
        SELECT Word
        FROM SplitWords
        WHERE rn = 1
    )
    SELECT @Result = STRING_AGG(Word, ' ')
    FROM UniqueWords;

    RETURN @Result;
END;

