response_examples = {
    "/": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "message",
                        "last_omsz_update": "2024-02-23T11:29:56.031130",
                        "last_mavir_update": "2024-02-23T11:29:56.031130"
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
                                "StartDate": "2005-07-27 18:10:00",
                                "EndDate": "2024-02-21 18:30:00",
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
                    "examples": {
                        "Specified Station": {
                            "value": {
                                "Message": "string",
                                "data": {
                                    0: "Time",
                                    1: "Prec",
                                    2: "Temp",
                                    "...": "..."
                                }
                            }
                        },
                        "Unspecified Station": {
                            "value": {
                                "Message": "string",
                                "data": {
                                    13704: {
                                        0: "Time",
                                        1: "Prec",
                                        2: "Temp",
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
                                    "2024-02-18 15:00:00": {
                                        "Prec": 0,
                                        "Temp": 10.7,
                                        "...": "..."
                                    },
                                    "2024-02-18 15:10:00": {
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
                                        "2024-02-18 15:00:00": {
                                            "Prec": 0,
                                            "Temp": 10.7,
                                            "...": "..."
                                        },
                                        "2024-02-18 15:10:00": {
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
    "/mavir/meta": {
        200: {
            "description": "Succesful Response",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "string",
                        "data": {
                            "NetPlanSystemProduction": {
                                "StartDate": "2011-11-01 23:10:00",
                                "EndDate": "2024-02-22 18:50:00",
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
                            "2024-02-18 15:00:00": {
                                "NetSystemLoad": 4717.373,
                                "NetSystemLoadFactPlantManagment": 4689.369,
                                "...": "..."
                            },
                            "2024-02-18 15:10:00": {
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
}