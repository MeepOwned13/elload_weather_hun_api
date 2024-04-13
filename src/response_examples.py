response_examples = {
    "/": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "message",
                        "last_omsz_update": "2024-02-23T11:29:56.031130",
                        "last_mavir_update": "2024-02-23T11:29:56.031130",
                        "last_s2s_update": "2024-02-23T11:10:56.031130"
                    }
                }
            }
        }
    },
    "/omsz/logo": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "https://www.met.hu/images/logo/omsz_logo_1362x492_300dpi.png"
                    }
                }
            }
        }
    },
    "/omsz/meta": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "string",
                        "data": {
                            13704: {
                                "Latitude": 47.6783,
                                "Longitude": 16.6022,
                                "Elevation": 232.8,
                                "StationName": "Sopron Kuruc-domb",
                                "RegioName": "Győr-Moson-Sopron"
                            },
                            13711: {
                                "...": "..."
                            }
                        }
                    }
                }
            }
        }
    },
    "/omsz/status": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "string",
                        "data": {
                            13704: {
                                "StartDate": "2005-07-27T18:10:00",
                                "EndDate": "2024-02-21T18:30:00",
                                "Latitude": 47.6783,
                                "Longitude": 16.6022,
                                "Elevation": 232.8,
                                "StationName": "Sopron Kuruc-domb",
                                "RegioName": "Győr-Moson-Sopron"
                            },
                            13711: {
                                "...": "..."
                            }
                        }
                    }
                }
            }
        }
    },
    "/omsz/columns": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "string",
                        "data": {
                            0: "Time",
                            1: "StationNumber",
                            2: "Prec",
                            3: "Temp",
                            "...": "..."
                        }
                    }
                }
            }
        },
        400: {
            "description": "Bad Request",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Error message"
                    }
                }
            }
        }
    },
    "/omsz/weather": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "examples": {
                        "Specified Station": {
                            "value": {
                                "Message": "string",
                                "data": {
                                    "2024-02-18T15:00:00": {
                                        "Prec": 0,
                                        "Temp": 10.7,
                                        "...": "..."
                                    },
                                    "2024-02-18T15:10:00": {
                                        "...": "..."
                                    },
                                    "...": "..."
                                }
                            }
                        },
                        "Unspecified Station": {
                            "value": {
                                "Message": "string",
                                "data": {
                                    13704: {
                                        "2024-02-18T15:00:00": {
                                            "Prec": 0,
                                            "Temp": 10.7,
                                            "...": "..."
                                        },
                                        "2024-02-18T15:10:00": {
                                            "..."
                                        }
                                    },
                                    13711: {
                                        "..."
                                    },
                                    "...": "..."
                                }
                            }
                        }
                    }
                }
            }
        },
        400: {
            "description": "Bad Request",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Error message"
                    }
                }
            }
        }
    },
    "/mavir/logo": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "https://www.mavir.hu/o/mavir-portal-theme/images/mavir_logo_white.png"
                    }
                }
            }
        }
    },
    "/mavir/status": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "string",
                        "data": {
                            "NetPlanSystemProduction": {
                                "StartDate": "2011-11-01T23:10:00",
                                "EndDate": "2024-02-22T18:50:00",
                            },
                            "NetSystemLoad": {
                                "...": "..."
                            },
                            "...": "..."
                        }
                    }
                }
            }
        }
    },
    "/mavir/columns": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "string",
                        "data": {
                            0: "Time",
                            1: "NetSystemLoad",
                            "...": "..."
                        }
                    }
                }
            }
        }
    },
    "/mavir/load": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "string",
                        "data": {
                            "2024-02-18T15:00:00": {
                                "NetSystemLoad": 4717.373,
                                "NetSystemLoadFactPlantManagment": 4689.369,
                                "...": "..."
                            },
                            "2024-02-18T15:10:00": {
                                "...": "..."
                            },
                            "...": "..."
                        }
                    }
                }
            }
        },
        400: {
            "description": "Bad Request",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Error message"
                    }
                }
            }
        }
    },
    "/ai/columns": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "string",
                        "data": {
                            0: "Time",
                            1: "NetSystemLoad",
                            2: "Prec",
                            "...": "..."
                        }
                    }
                }
            }
        }
    },
    "/ai/table": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "examples": {
                        "10min": {
                            "value": {
                                "data": {
                                    "2024-03-17T15:00:00": {
                                        "NetSystemLoad": 4242.404,
                                        "Prec": 0,
                                        "Temp": 11.35,
                                        "...": "..."
                                    },
                                    "2024-03-17T15:10:00": {
                                        "...": "..."
                                    }
                                }
                            }
                        },
                        "1hour": {
                            "value": {
                                "data": {
                                    "2024-02-18T15:00:00": {
                                        "NetSystemLoad": 4050.509,
                                        "Prec": 1.5,
                                        "Temp": 11.51,
                                        "...": "..."
                                    },
                                    "2024-02-18T16:00:00": {
                                        "...": "..."
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        400: {
            "description": "Bad Request",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Error message"
                    }
                }
            }
        }
    },
    "/ai/s2s/status": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "data": {
                            "S2S": {
                                "StartDate": "2017-01-01T00:00:00",
                                "EndDate": "2024-04-02T17:00:00",
                            }
                        }
                    }
                }
            }
        }
    },
    "/ai/s2s/preds": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "examples": {
                        "non-aligned": {
                            "value": {
                                "data": {
                                    "2020-06-23T16:00:00": {
                                        "NSLTplus1": 4982.53,
                                        "NSLTplus2": 5064.72,
                                        "NSLTplus3": 5075.42,
                                    },
                                    "2020-06-23T17:00:00": {
                                        "...": "..."
                                    }
                                }
                            }
                        },
                        "aligned": {
                            "value": {
                                "data": {
                                    "2020-06-23T17:00:00": {
                                        "NetSystemLoad": 4961.13,
                                        "NSLP1ago": 4982.53,
                                        "NSLP2ago": 4997.66,
                                        "NSLP3ago": 4946.14,
                                    },
                                    "2020-06-23T18:00:00": {
                                        "...": "..."
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        400: {
            "description": "Bad Request",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Error message"
                    }
                }
            }
        }
    }
}
