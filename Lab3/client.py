import grpc
import weather_pb2
import weather_pb2_grpc


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = weather_pb2_grpc.WeatherHistoryStub(channel)

        # --- Define Locations and Date Range ---
        locations = {
            "BGI": weather_pb2.Location(lat=13.0744, lon=-59.4922),
            "PUJ": weather_pb2.Location(lat=18.5675, lon=-68.3633),   
            "YGK": weather_pb2.Location(lat=44.2258, lon=-76.5967),   
        }
        reading_week = weather_pb2.DateRange(start_date='2024-02-19', end_date='2024-02-23')
        summer_break = weather_pb2.DateRange(start_date='2025-04-27', end_date='2025-09-02')

        # === REQUEST 1: GetSummary for Bridgetown, Barbados ===
        request = weather_pb2.Summary(location=locations['BGI'], daterange=reading_week)
        reply = stub.GetSummary(request)
        print(reply)

        # Use reading_week period

        # === REQUEST 2: GetSummary for Punta Cana, Dominican Republic ===
        request = weather_pb2.Summary(location=locations['PUJ'], daterange=reading_week)
        reply = stub.GetSummary(request)
        print(reply)

        # Use reading_week period

        # === REQUEST 3: StartPrecipitationAnalysis for Kingston, ON ===
        request = weather_pb2.Summary(location=locations['YGK'], daterange=summer_break)
        reply = stub.PrecipAnalysis(request)
        print(reply)
        # Use summer_break period

        # === REQUEST 4: GetPrecipitation for Kingston, ON ===
        request = weather_pb2.Summary(location=locations['YGK'], daterange=reading_week)
        reply = stub.GetSummary(request)
        print(reply)
        # Use less than 5 mm

        # === REQUEST 5: GetPrecipitation for Kingston, ON ===
        request = weather_pb2.Summary(location=locations['PUJ'], daterange=reading_week)
        reply = stub.GetSummary(request)
        print(reply)
        # Use equals 4.1 mm

        # === REQUEST 6: GetPrecipitation for Kingston, ON ===
        request = weather_pb2.Summary(location=locations['PUJ'], daterange=reading_week)
        reply = stub.GetSummary(request)
        print(reply)
        # Use greater than 10 mm


if __name__ == '__main__':
    run()