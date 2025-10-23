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
        print("-- Weather Summary for Bridgetown (BGI) --")
        print("Period: " + reading_week.start_date + " to " + reading_week.end_date)
        print("Avg Temp: " + str(round(reply.temperature_c, 2)) + "°C\nMax Wind: " + str(round(reply.wind, 2)) + " km/h\nTotal Rain: " + str(round(reply.precip, 2)) + " mm\n")

        # === REQUEST 2: GetSummary for Punta Cana, Dominican Republic ===
        request = weather_pb2.Summary(location=locations['PUJ'], daterange=reading_week)
        reply = stub.GetSummary(request)
        print("-- Weather Summary for Punta Cana (PUJ) --")
        print("Period: " + reading_week.start_date + " to " + reading_week.end_date)
        print("Avg Temp: " + str(round(reply.temperature_c, 2)) + "°C\nMax Wind: " + str(round(reply.wind, 2)) + " km/h\nTotal Rain: " + str(round(reply.precip, 2)) + " mm\n")

        # === REQUEST 3: StartPrecipitationAnalysis for Kingston, ON ===
        request = weather_pb2.AnalysisSummary(location=locations['YGK'], daterange=summer_break, loc_name='YGK')
        reply = stub.PrecipAnalysis(request)

        # Use summer_break period

        # === REQUEST 4: GetPrecipitation for Kingston, ON ===
        request = weather_pb2.Precip(loc_name='YGK', precip=5.0, operator='>')
        reply = stub.GetPrecip(request)
        print("-- Precipitation less than 5mm in Kingston (YGK) --\nPeriod: " + summer_break.start_date + " to " + summer_break.end_date)
        print("Number of days: " + str(reply.match_days) + "\nMaximum amount: " + str(round(reply.max_precip, 2)) + "\nDay of maximum amount: " + reply.max_precip_date)
        print("List of days: ")
        for i in reply.return_list:
            print("- " + i.date)
        # Use less than 5 mm

        # === REQUEST 5: GetPrecipitation for Kingston, ON ===
        request = weather_pb2.Precip(loc_name='YGK', precip=4.1, operator='=')
        reply = stub.GetPrecip(request)
        print("-- Precipitation equal to 4.1mm in Kingston (YGK) --\nPeriod: " + summer_break.start_date + " to " + summer_break.end_date)
        print("Number of days: " + str(reply.match_days) + "\nMaximum amount: " + str(
            round(reply.max_precip, 2)) + "\nDay of maximum amount: " + reply.max_precip_date)
        print("List of days: ")
        for i in reply.return_list:
            print("- " + i.date)
        # Use equals 4.1 mm

        # === REQUEST 6: GetPrecipitation for Kingston, ON ===
        request = weather_pb2.Precip(loc_name='YGK', precip=10, operator='<')
        reply = stub.GetPrecip(request)
        print("-- Precipitation more than 10mm in Kingston (YGK) --\nPeriod: " + summer_break.start_date + " to " + summer_break.end_date)
        print("Number of days: " + str(reply.match_days) + "\nMaximum amount: " + str(
            round(reply.max_precip, 2)) + "\nDay of maximum amount: " + reply.max_precip_date)
        print("List of days: ")
        for i in reply.return_list:
            print("- " + i.date)
        # Use greater than 10 mm


if __name__ == '__main__':
    run()