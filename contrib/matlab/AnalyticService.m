% AnalyticService provides a service that can receive requests on a named pipe
% and send responses over a different named pipe.
%
% If no pipe file names are given, then they are taken from environment variables:
%
%   input:  ANALYTIC_FIFO_IN
%   output: ANALYTIC_FIFO_OUT
%
% These are used if AnalyticService is created with no arguments. Otherwise, specify
% the input and output FIFO names like this:
%
%   svc = AnalyticService(inName, outName)
%
% Complete exmaple:
%
%   svc = AnalyticService;
%   svc.registerImageManipulation(@(req) imgmanip(req))
%   svc.run
%
%   function resp = imgmanip(req)
%     resp.score = 0.5;
%     resp.confidence = 0.1;
%   end
%
% The output JSON is wrapped for you - no need to wrap it for what the child FIFO
% runner expects; just produce a MATLAB struct that mirrors the desired response
% protobuf, and all of the other conversions will be done for you.
%
% Similarly, the request object that comes into your registered function is just a
% MATLAB struct with fields that match the camel-case versions of proto field names.
classdef AnalyticService < handle
  properties
    inFile
    outFile
    funcs = containers.Map;
  end
  methods
    function obj = AnalyticService(inFile, outFile)
      if nargin < 2
        outFile = getenv('ANALYTIC_FIFO_OUT');
      end
      if nargin < 1
        inFile = getenv('ANALYTIC_FIFO_IN');
      end
      obj.inFile = inFile;
      obj.outFile = outFile;
    end

    function registerImageManipulation(obj, f)
      obj.funcs('imgmanip') = f;
    end

    function registerVideoManipulation(obj, f)
      obj.funcs('vidmanip') = f;
    end

    function registerImageSplice(obj, f)
      obj.funcs('imgsplice') = f;
    end

    function registerImageCameraMatch(obj, f)
      obj.funcs('imgcammatch') = f;
    end

    function err(obj, out, code, msg)
      fprintf(out, '%s\n', jsonencode(struct('code', code, 'value', msg)));
    end

    function ok(obj, out, val)
      fprintf(out, '%s\n', jsonencode(struct('code', 'OK', 'value', val)));
    end

    function run(obj)
      try
        in = fopen(obj.inFile, 'r+');
        out = fopen(obj.outFile, 'w');
        nl = newline;
        line = '';
        while ~feof(in)
          % Can't just use fgetl, because it doesn't return from a fifo
          % right away. You have to specify length to get that on a
          % non-empty fifo. The fgets function has a similar problem: newlines
          % block even when reading exactly one character.
          ch = char(fread(in, 1));
          if ch ~= nl
            line = [line ch];
            continue
          end

          try
            msg = jsondecode(line);
          catch exc
            obj.err(out, 'INVALID_ARGUMENT', ['invalid json: ' line]);
            rethrow(exc)
          end

          if obj.funcs.isKey(msg.type)
            f = obj.funcs(msg.type);
            if ~isa(f, 'function_handle')
              obj.err(out, 'INTERNAL', ['handler for type ' msg.type ' is not a function'])
              error('non-function handler')
            end

            try
              resp = f(msg.value);
              obj.ok(out, resp);
            catch exc
              obj.err(out, 'INTERNAL', getReport(exc));
            end
          else
            obj.err(out, 'UNIMPLEMENTED', ['no handler for type ' msg.type]);
          end

          line = '';
        end
      catch exc
        disp(exc);
      end
      fclose(in);
      fclose(out);
    end
  end
end

