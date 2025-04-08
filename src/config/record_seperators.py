def map_record_separator(separator):
    mapping = {
        "CRLF": "\r\n",
        "LF": "\n",
        "CR": "\r",
        "LFCR": "\n\r",
        "FF": "\f",
        "EmptyLine": "\n\n",
        "None": "",
        "\r\n": "CRLF",
        "\n": "LF",
        "\r": "CR",
        "\n\r": "LFCR",
        "\f": "FF",
        "\n\n": "EmptyLine",
        "": "None"
    }
    return mapping.get(separator, separator)

#print(repr(map_record_separator("CRLF")))