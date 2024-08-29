
INSERT INTO repo_tenant (tenantid, name, description)
VALUES (
  0, 'MALLAR', 'Mallar'
),(
  1, 'SCRATCH', 'Lekstuga'
)
;


INSERT INTO repo_namespace (alias, namespace)
VALUES (
    'dc', 'http://purl.org/dc/elements/1.1'
)
;


INSERT INTO repo_attribute (attrid, qualname, attrname, attrtype, scalar)
VALUES (
  1, 'http://purl.org/dc/elements/1.1/title', 'dc:title', 1, TRUE  -- scalar
),(
  2, 'http://purl.org/dc/elements/1.1/creator', 'dc:creator', 1, FALSE -- vector
),(
  3, 'http://purl.org/dc/elements/1.1/subject', 'dc:subject', 1, FALSE -- vector
),(
  4, 'http://purl.org/dc/elements/1.1/description', 'dc:description', 1, FALSE -- vector
),(
  5, 'http://purl.org/dc/elements/1.1/publisher', 'dc:publisher', 1, FALSE -- vector
),(
  6, 'http://purl.org/dc/elements/1.1/contributors', 'dc:contributors', 1, FALSE -- vector
),(
  7, 'http://purl.org/dc/elements/1.1/date', 'dc:date', 2, FALSE -- vector
),(
  8, 'http://purl.org/dc/elements/1.1/type', 'dc:type', 1, FALSE -- vector
),(
  9, 'http://purl.org/dc/elements/1.1/format', 'dc:format', 1, FALSE -- vector
),(
  10, 'http://purl.org/dc/elements/1.1/identifier', 'dc:identifier', 1, TRUE -- scalar
),(
  11, 'http://purl.org/dc/elements/1.1/source', 'dc:source', 1, FALSE -- vector
),(
  12, 'http://purl.org/dc/elements/1.1/language', 'dc:language', 1, FALSE -- vector
),(
  13, 'http://purl.org/dc/elements/1.1/relation', 'dc:relation', 1, FALSE -- vector
),(
  14, 'http://purl.org/dc/elements/1.1/coverage', 'dc:coverage', 1, FALSE -- vector
),(
  15, 'http://purl.org/dc/elements/1.1/rights-management', 'dc:rights-management', 1, FALSE -- vector
)
;

INSERT INTO repo_attribute_description (attrid, lang, alias, description)
VALUES (
    1, 'en', 'Title', 'The name given to the resource. It''s a human-readable identifier that provides a concise representation of the resource''s content.'
),(
    1, 'se', 'Titel', 'Namnet som ges till resursen. Det är en människoläsbar identifierare som ger en kortfattad representation av resursens innehåll.'
),(
	2, 'en', 'Creator', 'An entity primarily responsible for making the resource, typically an individual, organization, or service.'
),(
	2, 'se', 'Skapare', 'En entitet som primärt är ansvarig för att skapa resursen, vanligtvis en individ, organisation eller tjänst.'
),(
	3, 'en', 'Subject', 'The topic or the focus of the content within the resource, often represented by keywords, phrases, or classification codes.'
),(
	3, 'se', 'Ämne', 'Ämnet eller fokuset för innehållet inom resursen, ofta representerat av nyckelord, fraser eller klassifikationskoder.'
),(
	4, 'en', 'Description', 'A summary or abstract detailing the content, purpose, or scope of the resource, providing insight into its primary features.'
),(
	4, 'se', 'Beskrivning', 'En sammanfattning eller abstrakt som beskriver innehållet, syftet eller omfånget av resursen och ger insikt i dess huvudsakliga egenskaper.'
),(
	5, 'en', 'Publisher', 'The entity responsible for making the resource available, which could be a person, organization, or service.'
),(
	5, 'se', 'Utgivare', 'Den entitet som ansvarar för att göra resursen tillgänglig, vilket kan vara en person, organisation eller tjänst.'
),(
	6, 'en', 'Contributor', 'Entities that have contributed to the creation or modification of the resource, but are not the primary creators.'
),(
	6, 'se', 'Medverkande', 'Entiteter som har bidragit till skapandet eller modifieringen av resursen, men som inte är de primära skaparna.'
),(
	7, 'en', 'Date', 'A point or period of time associated with the lifecycle of the resource, such as creation, publication, or modification date, often represented in ISO 8601 format.'
),(
	7, 'se', 'Datum', 'En tidpunkt eller tidsperiod associerad med resursens livscykel, som skapande-, publicerings- eller ändringsdatum, ofta representerat i ISO 8601-format.'
),(
	8, 'en', 'Type', 'The nature or genre of the content within the resource, such as text, image, video, dataset, etc., aiding in understanding its format and use.'
),(
	8, 'se', 'Typ', 'Innehållets natur eller genre inom resursen, såsom text, bild, video, dataset, etc., vilket hjälper till att förstå dess format och användning.'
),(
	9, 'en', 'Format', 'The file format, physical medium, or dimensions of the resource, often standardized using MIME types (e.g., text/html, image/jpeg).'
),(
	9, 'se', 'Format', 'Filformatet, det fysiska mediet eller dimensionerna av resursen, ofta standardiserade med MIME-typer (t.ex. text/html, image/jpeg).'
),(
	10, 'en', 'Identifier', 'A unique reference to the resource within a given context, such as a URL, DOI, ISBN, or URN, enabling unambiguous identification and access.'
),(
	10, 'se', 'Identifierare', 'En unik referens till resursen inom ett givet sammanhang, såsom en URL, DOI, ISBN eller URN, som möjliggör entydig identifiering och åtkomst.'
),(
	11, 'en', 'Source', 'The resource from which the current resource is derived or generated, providing context on its origins or predecessors.'
),(
	11, 'se', 'Källa', 'Resursen från vilken den aktuella resursen är härledd eller genererad, vilket ger kontext om dess ursprung eller föregångare.'
),(
	12, 'en', 'Language', 'The primary language(s) of the content, often denoted using ISO 639 codes (e.g., en for English, fr for French).'
),(
	12, 'se', 'Språk', 'Huvudspråket (eller språken) för innehållet, ofta angivet med ISO 639-koder (t.ex. en för engelska, fr för franska).'
),(
	13, 'en', 'Relation', 'A reference to a related resource, indicating a connection or association, such as a parent, child, or sibling relationship.'
),(
	13, 'se', 'Relation', 'En referens till en relaterad resurs, som anger en koppling eller association, såsom en föräldra-, barn- eller syskonrelation.'
),(
	14, 'en', 'Coverage', 'The spatial or temporal extent of the content, indicating geographical locations or time periods relevant to the resource.'
),(
	14, 'se', 'Täckning', 'Den rumsliga eller tidsmässiga omfattningen av innehållet, som anger geografiska platser eller tidsperioder som är relevanta för resursen.'
),(
	15, 'en', 'Rights', 'Information about the rights held in and over the resource, such as copyright status, licensing terms, or access restrictions, ensuring proper usage and distribution.'
),(
	15, 'se', 'Rättigheter', 'Information om rättigheterna som innehas i och över resursen, såsom upphovsrättsstatus, licensvillkor eller åtkomstbegränsningar, vilket säkerställer korrekt användning och distribution.'
)
;
