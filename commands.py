def data_extraction_internal():
    from src.data_extraction_service import main
    main()


def data_extraction_external():
    # @LYF Deal with this function
    from src.data_extraction_service.external.main import main
    main()
    pass

def shapeimgae_generator(schedule=False):
    from src.shapeimage_generation_script.shapeimgae_generator import main_schedule, main
    if schedule:
        main_schedule()
    else:
        main()

def run_web():
    from src.geocloudservice.web import main
    main()