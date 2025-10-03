import grpc
from concurrent import futures

import utils
import weather_pb2
import weather_pb2_grpc


class WeatherHistoryServicer(weather_pb2_grpc.WeatherHistoryServicer):

    def GetSummary(self, request, context):
        return weather_pb2.SummaryReturn(temperature_c="test msg")



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