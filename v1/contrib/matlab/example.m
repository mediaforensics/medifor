import AnalyticService;

svc = AnalyticService;
svc.registerImageManipulation(@(req) ImgManip(req));
svc.run;

function resp = ImgManip(req)
  resp.score = 0.5;
end
