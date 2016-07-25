package com.hd.controller;

import java.io.IOException;

import javax.validation.Valid;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.hd.form.ConfigurationForm;

@Controller
public class Home {

//	@RequestMapping(value = "/", method = RequestMethod.GET)
//	public String index(ConfigurationForm post, BindingResult bindingResult, Model model) throws Exception {
//		am.init();
//		model.addAttribute("cores", am.getCores());
//		model.addAttribute("memory", am.getMemory());
//		model.addAttribute("priority", am.getPriority());
//		model.addAttribute("containersNumber", am.getContainersNumber());
//		model.addAttribute("comand", am.getCommand());
//
//		return "index";
//	}
//
//	@RequestMapping(value = "/", method = RequestMethod.POST)
//	public String addNewPost(@Valid ConfigurationForm post, BindingResult bindingResult, Model model) throws YarnException, InterruptedException, IOException {
//		if (bindingResult.hasErrors()) {
//			return "index";
//		}
//		am.setCores(post.getCores());
//		am.setMemory(post.getMemory());
//		am.setMemory(post.getPriority());
//		am.setPriority(post.getConainersNumber());
//		am.submitContainers();
//		return "index";
//	}
}