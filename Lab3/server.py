import grpc
from concurrent import futures

import utils
import weather_pb2
import weather_pb2_grpc


class WeatherHistoryServicer(weather_pb2_grpc.WeatherHistoryServicer):


    def __init__(self):
        self.list = {}

    def GetSummary(self, request, context):
        temp, wind, precip = utils.fetch_summary_data(request.location, request.daterange)
        return weather_pb2.SummaryReturn(temperature_c=temp, wind=wind, precip=precip)

    def PrecipAnalysis(self, request, context):
        events = utils.fetch_analysis_data(request.location, request.daterange)
        self.list[request.loc_name] = events
        return weather_pb2.AnalysisReturn(events=events)

    def GetPrecip(self, request, context):
        if not self.list[request.loc_name]:
            return ValueError

        return_list = []
        match_days = 0
        max_precip = 0.0
        max_precip_date = ''

        for i in self.list[request.loc_name]:
            if request.operator == '=':
                if i.precipitation_mm == request.precip:
                    return_list.append(i)
                    match_days += 1
                    if i.precipitation_mm > max_precip:
                        max_precip = i.precipitation_mm
                        max_precip_date = i.date
            elif request.operator == '>':
                if i.precipitation_mm < request.precip:
                    return_list.append(i)
                    match_days += 1
                    if i.precipitation_mm > max_precip:
                        max_precip = i.precipitation_mm
                        max_precip_date = i.date
            elif request.operator == '<':
                if i.precipitation_mm > request.precip:
                    return_list.append(i)
                    match_days += 1
                    if i.precipitation_mm > max_precip:
                        max_precip = i.precipitation_mm
                        max_precip_date = i.date

        return weather_pb2.GetPrecipReturn(return_list=return_list, match_days=match_days, max_precip=max_precip, max_precip_date=max_precip_date)

def serve():
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    weather_pb2_grpc.add_WeatherHistoryServicer_to_server(WeatherHistoryServicer(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == '__main__':
    serve()